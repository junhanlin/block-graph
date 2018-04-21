package com.example.blockgraph;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.security.interfaces.RSAKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.example.blockgraph.WalletMetaHelper.WalletMeta;

import info.blockchain.api.blockexplorer.Block;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.omg.CORBA.INTERNAL;

public class BlockWritter
{

    public static void main(String[] args) throws ConfigurationException, ClassNotFoundException
    {
	Class.forName("com.mysql.jdbc.Driver");

	Parameters parameters = new Parameters();
	File propertiesFile = new File(args[0]);

	FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
		.configure(parameters.fileBased().setFile(propertiesFile));
	Configuration config = builder.getConfiguration();

	String awsAccountKey = (String) config.getProperty("aws.account.key");
	String awsAccountSecret = (String) config.getProperty("aws.account.secret");
	String bucketName = (String) config.getProperty("aws.s3.bucket");

	AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(awsAccountKey, awsAccountSecret));

	String neo4jBolt = (String) config.getProperty("neo4j.bolt");
	String neo4jUser = (String) config.getProperty("neo4j.user");
	String neo4jPassword = (String) config.getProperty("neo4j.password");

	final Driver driver = GraphDatabase.driver(neo4jBolt, AuthTokens.basic(neo4jUser, neo4jPassword));
	Kryo kryo = new Kryo();
	kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

	try (Session session = driver.session())
	{
	    final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(1000);
	    ListObjectsV2Result result;
	    do
	    {
		result = s3Client.listObjectsV2(req);

		for (S3ObjectSummary objectSummary : result.getObjectSummaries())
		{
		    String fileKey = objectSummary.getKey();

		    if (fileKey.startsWith("height-"))
		    {
			long height = Long.parseLong(fileKey.substring(fileKey.indexOf("-") + 1, fileKey.length()));

			if (session.run("MATCH (bh:BlockHeight) WHERE bh.height=" + height + " RETURN bh;").hasNext())
			{
			    continue;
			}

			final BlockHeight blockHeightObj;
			byte[] fileBytes = null;
			try (S3ObjectInputStream fileIn = s3Client.getObject(bucketName, fileKey).getObjectContent())
			{
			    fileBytes = IOUtils.toByteArray(fileIn);

			}
			catch (SdkClientException | IOException e1)
			{
			    // TODO Auto-generated catch block
			    continue;
			}
			try (com.esotericsoftware.kryo.io.Input kryoInput = new com.esotericsoftware.kryo.io.Input(new ByteArrayInputStream(fileBytes)))
			{
			    blockHeightObj = kryo.readObject(kryoInput, BlockHeight.class);
			}
			catch (KryoException e)
			{
			    e.printStackTrace();
			    continue;
			}

			

			for (Block block : blockHeightObj.getBlocks())
			{

			    txLoop: for (info.blockchain.api.blockexplorer.Transaction blockTx : block.getTransactions())
			    {
				session.writeTransaction(new TransactionWork<Void>()
				{
				    @Override
				    public Void execute(Transaction tx)
				    {

					Map<String, BigDecimal> inputMap = new HashMap<>();
					for (info.blockchain.api.blockexplorer.Input input : blockTx.getInputs())
					{
					    if (input.getPreviousOutput() == null)
					    {
						return null;
					    }

					    String addr = input.getPreviousOutput().getAddress();
					    long value = input.getPreviousOutput().getValue();
					    if (addr.trim().isEmpty())
					    {
						// Unable to decode address
						// e.g.
						// 862d8672ffba284095df0228544bdef849ce6fc74b73fc478c01472edc842d04
						continue;
					    }
					    if (!inputMap.containsKey(addr))
					    {
						inputMap.put(addr, BigDecimal.ZERO);
					    }
					    inputMap.put(addr, inputMap.get(addr).add(BigDecimal.valueOf(value)));

					}

					Map<String, BigDecimal> outputMap = new HashMap<>();
					for (info.blockchain.api.blockexplorer.Output output : blockTx.getOutputs())
					{
					    String addr = output.getAddress();
					    long value = output.getValue();
					    if (addr.trim().isEmpty())
					    {
						// Unable to decode address
						continue;
					    }
					    if (!outputMap.containsKey(addr))
					    {
						outputMap.put(addr, BigDecimal.ZERO);
					    }
					    outputMap.put(addr, outputMap.get(addr).add(BigDecimal.valueOf(value)));

					}

					if (inputMap.isEmpty() || outputMap.isEmpty())
					{
					    return null;
					}

					BigDecimal totalInput = inputMap.values().stream().reduce(BigDecimal.ZERO, (a, b) -> a.add(b));
					BigDecimal totalOutput = outputMap.values().stream().reduce(BigDecimal.ZERO, (a, b) -> a.add(b));
					BigDecimal fee = totalInput.subtract(totalOutput);
					if (fee.compareTo(BigDecimal.ZERO) > 0)
					{
					    for (String inputAddr : inputMap.keySet())
					    {
						BigDecimal amt = inputMap.get(inputAddr);
						BigDecimal distributedFee = fee.multiply(amt.divide(totalInput, 19, BigDecimal.ROUND_DOWN));
						inputMap.put(inputAddr, amt.subtract(distributedFee));
					    }
					}

					Set<String> intersectedAddresses = new HashSet<>(inputMap.keySet());
					intersectedAddresses.retainAll(outputMap.keySet());

					for (String intersectedAddress : intersectedAddresses)
					{
					    // check symbol.....

					    BigDecimal leftVal = inputMap.get(intersectedAddress);
					    BigDecimal rightVal = outputMap.get(intersectedAddress);

					    if (leftVal.compareTo(rightVal) > 0)
					    {
						inputMap.put(intersectedAddress, leftVal.subtract(rightVal));
						outputMap.remove(intersectedAddress);
					    }
					    else if (leftVal.compareTo(rightVal) < 0)
					    {
						inputMap.remove(intersectedAddress);
						outputMap.put(intersectedAddress, rightVal.subtract(leftVal));
					    }
					    else
					    {
						inputMap.remove(intersectedAddress);
						outputMap.remove(intersectedAddress);
					    }

					}

					Map<String, BigDecimal> ratioMap = new HashMap<>();
					for (Entry<String, BigDecimal> outputEntry : outputMap.entrySet())
					{
					    if (totalOutput.compareTo(BigDecimal.ZERO) == 0)
					    {
						System.out.println("totalOutput is zero!!!");
					    }
					    BigDecimal ratio = outputEntry.getValue().divide(totalOutput, 19, BigDecimal.ROUND_DOWN);
					    ratioMap.put(outputEntry.getKey(), ratio);
					}

					for (Entry<String, BigDecimal> inputEntry : inputMap.entrySet())
					{
					    String inputAddr = inputEntry.getKey();
					    BigDecimal inputValue = inputEntry.getValue();

					    WalletMeta inputWallet = null;
					    try
					    {
						inputWallet = new WalletMetaHelper(config, inputAddr).getMeta();

					    }
					    catch (IOException | SQLException e)
					    {
						e.printStackTrace();
						continue;
					    }

					    for (Entry<String, BigDecimal> ratioEntry : ratioMap.entrySet())
					    {

						String outputAddr = ratioEntry.getKey();
						if (outputAddr == null || outputAddr.trim().isEmpty())
						{
						    System.out.println();
						}

						BigDecimal weightedOutputValue = inputValue.multiply(ratioEntry.getValue());

						WalletMeta outputWallet = null;
						try
						{
						    outputWallet = new WalletMetaHelper(config, outputAddr).getMeta();
						}
						catch (IOException | SQLException e)
						{
						    e.printStackTrace();
						    continue;
						}
						String queryMergeM = " MERGE (m:Address" + (inputWallet.getCategory() == null ? "" : ":" + inputWallet.getCategory() + " ")
							+ " {address: '" + inputAddr + "'}) ";
						String queryUpdateM = "\n ON CREATE SET m += { balance:" + inputWallet.getBalance()
							+ (inputWallet.getGroup() == null ? "" : " ,groupName:'" + inputWallet.getGroup() + "' ") + "}";

						String queryMergeN = "\nMERGE (n:Address" + (outputWallet.getCategory() == null ? "" : ":" + outputWallet.getCategory() + " ")
							+ " {address: '" + outputAddr + "'}) ";
						String queryUpdateN = "\n ON CREATE SET n += { balance:" + outputWallet.getBalance()
							+ (outputWallet.getGroup() == null ? "" : " ,groupName:'" + outputWallet.getGroup() + "' ") + "}";

						String queryPay = "\nMERGE (m)-[:PAY {amount:'" + weightedOutputValue.toPlainString() + "', weight: '"
							+ ratioEntry.getValue().toPlainString() + "', time:" + blockTx.getTime() + ", txHash:'" + blockTx.getHash()
							+ "', blockHeight: " + block.getHeight() + ", blockHash:'" + block.getHash() + "'}]->(n)";

						String query = queryMergeM + queryUpdateM + queryMergeN + queryUpdateN + queryPay;
						System.out.println(query);
						tx.run(query);

					    }
					}

					return null;
				    }
				});
			    }
			}
			
			{
			    Map<String, Object> params = new HashMap<>();
			    params.put("height", height);
			    session.run("CREATE (bh:BlockHeight) SET bh.height = $height ", params);
			}

		    }
		}
		System.out.println("Next Continuation Token : " + result.getNextContinuationToken());
		req.setContinuationToken(result.getNextContinuationToken());
	    } while (result.isTruncated() == true);

	}

    }
}
