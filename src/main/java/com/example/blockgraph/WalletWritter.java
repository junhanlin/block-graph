package com.example.blockgraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.types.Node;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.esotericsoftware.kryo.Kryo;
import com.example.blockgraph.WalletMetaHelper.WalletMeta;
import com.mysql.fabric.xmlrpc.base.Array;

import info.blockchain.api.APIException;
import info.blockchain.api.blockexplorer.Address;
import info.blockchain.api.blockexplorer.Block;
import info.blockchain.api.blockexplorer.BlockExplorer;

public class WalletWritter
{
    public static int MAX_DEPTH = 2;

    public static void main(String[] args) throws ConfigurationException, APIException, IOException
    {
	Parameters parameters = new Parameters();
	File propertiesFile = new File(args[0]);

	FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
		.configure(parameters.fileBased().setFile(propertiesFile));
	Configuration config = builder.getConfiguration();

	BlockExplorer blockExplorer = new BlockExplorer();

	Scanner sc = new Scanner(System.in);

	
	while (sc.hasNext())
	{
	    String addr = sc.next();
	    if (addr.equals("exit"))
	    {
		break;
	    }
	    LinkedList<AddressDepth> queue = new LinkedList<>();
	    queue.offer(new AddressDepth(addr, 0));
	    while (queue.size() > 0)
	    {
		System.out.println("Queue size = " + queue.size());
		AddressDepth addressDepth = queue.poll();

		queue.addAll(process(config, blockExplorer, addressDepth));
	    }
	    
	    System.out.println("Enter next seed address: ");
	}

    }

    public static List<AddressDepth> process(Configuration config, BlockExplorer blockExplorer, AddressDepth addressDepth) throws APIException, IOException
    {
	if (addressDepth.depth > MAX_DEPTH)
	{
	    return Collections.emptyList();
	}
	String neo4jBolt = (String) config.getProperty("neo4j.bolt");
	String neo4jUser = (String) config.getProperty("neo4j.user");
	String neo4jPassword = (String) config.getProperty("neo4j.password");

	final Driver driver = GraphDatabase.driver(neo4jBolt, AuthTokens.basic(neo4jUser, neo4jPassword));
	Kryo kryo = new Kryo();
	kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

	try (Session session = driver.session())
	{
	    StatementResult result = session.run("MATCH (n:Address) WHERE n.address='" + addressDepth.address + "' AND n.visited=true RETURN n");
	    if (result.hasNext())
	    {
		return Collections.emptyList();
	    }

	    Address address = blockExplorer.getAddress(addressDepth.address);
	    List<AddressDepth> retVal = new ArrayList<>();
	    for (info.blockchain.api.blockexplorer.Transaction addrTx : address.getTransactions())
	    {
		final Map<String, BigDecimal> inputMap = new HashMap<>();
		final Map<String, BigDecimal> outputMap = new HashMap<>();
		for (info.blockchain.api.blockexplorer.Input input : addrTx.getInputs())
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

		for (info.blockchain.api.blockexplorer.Output output : addrTx.getOutputs())
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

		    if (inputMap.containsKey(address.getAddress()) && !inputAddr.equals(address.getAddress()))
		    {
			continue;
		    }

		    WalletMeta inputWallet = null;
		    try
		    {
			inputWallet = new WalletMetaHelper(config, inputAddr).getMeta();

		    }
		    catch (IOException | SQLException | IndexOutOfBoundsException e)
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
			if (!inputAddr.equals(address.getAddress()) && !outputAddr.equals(address.getAddress()))
			{
			    continue;
			}
			BigDecimal weightedOutputValue = inputValue.multiply(ratioEntry.getValue());

			WalletMeta outputWallet = null;
			try
			{
			    outputWallet = new WalletMetaHelper(config, outputAddr).getMeta();
			}
			catch (IOException | SQLException | IndexOutOfBoundsException e)
			{
			    e.printStackTrace();
			    continue;
			}
			String queryMergeM = " MERGE (m:Address" + (inputWallet.getCategory() == null ? "" : ":" + inputWallet.getCategory() + " ") + " {address: '" + inputAddr
				+ "'}) ";
			String queryUpdateM = "\n ON CREATE SET m += { balance:" + inputWallet.getBalance()
				+ (inputWallet.getGroup() == null ? "" : " ,groupName:'" + inputWallet.getGroup() + "' ") + " ,visited:false}";

			String queryMergeN = "\nMERGE (n:Address" + (outputWallet.getCategory() == null ? "" : ":" + outputWallet.getCategory() + " ") + " {address: '" + outputAddr
				+ "'}) ";
			String queryUpdateN = "\n ON CREATE SET n += { balance:" + outputWallet.getBalance()
				+ (outputWallet.getGroup() == null ? "" : " ,groupName:'" + outputWallet.getGroup() + "' ") + " ,visited:false}";

			String queryPay = "\nMERGE (m)-[:PAY {amount:'" + weightedOutputValue.toPlainString() + "', weight: '" + ratioEntry.getValue().toPlainString() + "', time:"
				+ addrTx.getTime() + ", txHash:'" + addrTx.getHash() + "', blockHeight: " + addrTx.getBlockHeight() + "}]->(n)";

			String query = queryMergeM + queryUpdateM + queryMergeN + queryUpdateN + queryPay;
			System.out.println(query);
			session.run(query);

		    }
		}

		session.run("MATCH (n:Address {address:'" + addressDepth.address + "'}) SET n.visited = true RETURN n");

		if (inputMap.containsKey(addressDepth.address))
		{
		    retVal.addAll(outputMap.keySet().stream().map(a -> new AddressDepth(a, addressDepth.depth + 1)).collect(Collectors.toList()));
		}
		else if (outputMap.containsKey(addressDepth.address))
		{
		    retVal.addAll(inputMap.keySet().stream().map(a -> new AddressDepth(a, addressDepth.depth + 1)).collect(Collectors.toList()));
		}

	    }
	    return retVal;

	}
    }

    public static class AddressDepth
    {
	public String address;
	public int depth;

	public AddressDepth(String address, int depth)
	{
	    super();
	    this.address = address;
	    this.depth = depth;
	}

    }

}
