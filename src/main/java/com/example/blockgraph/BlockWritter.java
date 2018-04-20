package com.example.blockgraph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;

import info.blockchain.api.blockexplorer.Block;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class BlockWritter
{

    public static void main(String[] args)
    {
	final Driver driver = GraphDatabase.driver("bolt://srv.yamkr.com:7687", AuthTokens.basic("neo4j", "1qaz2wsx"));
	Kryo kryo = new Kryo();
	kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

	File blockDir = new File("/Users/johnlin/Documents/Blocks");

	try (Session session = driver.session())
	{
	    File[] blockHeightFiles = blockDir.listFiles();
	    for (File blockHeightFile : blockHeightFiles)
	    {
		String fileName = blockHeightFile.getName();
		if (blockHeightFile.isFile() && !fileName.endsWith(".part") && fileName.startsWith("height-"))
		{
		    long height = Long.parseLong(fileName.substring(fileName.indexOf("-") + 1, fileName.length()));

		    if (session.run("MATCH (bh:BlockHeight) WHERE bh.height=" + height + " RETURN bh;").hasNext())
		    {
			continue;
		    }

		    final BlockHeight blockHeightObj;
		    try (com.esotericsoftware.kryo.io.Input kryoInput = new com.esotericsoftware.kryo.io.Input(new FileInputStream(blockHeightFile)))
		    {
			blockHeightObj = kryo.readObject(kryoInput, BlockHeight.class);
		    }
		    catch (FileNotFoundException | KryoException e)
		    {
			e.printStackTrace();
			continue;
		    }

		    session.writeTransaction(new TransactionWork<Void>()
		    {
			@Override
			public Void execute(Transaction tx)
			{
			    {
				Map<String, Object> params = new HashMap<>();
				params.put("height", height);
				tx.run("CREATE (bh:BlockHeight) SET bh.height = $height ", params);
			    }

			    for (Block block : blockHeightObj.getBlocks())
			    {
				txLoop: for (info.blockchain.api.blockexplorer.Transaction blockTx : block.getTransactions())
				{
				    Map<String, BigDecimal> inputMap = new HashMap<>();
				    for (info.blockchain.api.blockexplorer.Input input : blockTx.getInputs())
				    {
					if (input.getPreviousOutput() == null)
					{
					    continue txLoop;
					}
					
					String addr = input.getPreviousOutput().getAddress();
					long value = input.getPreviousOutput().getValue();
					if(addr.trim().isEmpty())
					{
					    // Unable to decode address
					    // e.g. 862d8672ffba284095df0228544bdef849ce6fc74b73fc478c01472edc842d04
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
					if(addr.trim().isEmpty())
					{
					    //Unable to decode address
					    continue;
					}
					if (!outputMap.containsKey(addr))
					{
					    outputMap.put(addr, BigDecimal.ZERO);
					}
					outputMap.put(addr, outputMap.get(addr).add(BigDecimal.valueOf(value)));

				    }
				    
				    if(inputMap.isEmpty() || outputMap.isEmpty())
				    {
					continue txLoop;
				    }

				    BigDecimal totalInput = inputMap.values().stream().reduce(BigDecimal.ZERO, (a, b) -> a.add(b));
				    BigDecimal totalOutput = outputMap.values().stream().reduce(BigDecimal.ZERO, (a, b) -> a.add(b));
				    BigDecimal fee = totalInput.subtract(totalOutput);
				    if (fee.compareTo(BigDecimal.ZERO) > 0)
				    {
					for (String inputAddr : inputMap.keySet())
					{
					    BigDecimal amt = inputMap.get(inputAddr);
					    BigDecimal distributedFee = fee.multiply(amt.divide(totalInput,19,BigDecimal.ROUND_DOWN));
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
					BigDecimal ratio = outputEntry.getValue().divide(totalOutput,19,BigDecimal.ROUND_DOWN);
					ratioMap.put(outputEntry.getKey(), ratio);
				    }

				    for (Entry<String, BigDecimal> inputEntry : inputMap.entrySet())
				    {
					String inputAddr = inputEntry.getKey();
					BigDecimal inputValue = inputEntry.getValue();
					for (Entry<String, BigDecimal> ratioEntry : ratioMap.entrySet())
					{
					   
					    String outputAddr = ratioEntry.getKey();
					    if(outputAddr == null || outputAddr.trim().isEmpty())
					    {
						System.out.println();
					    }
					    BigDecimal weightedOutputValue = inputValue.multiply(ratioEntry.getValue());
					    String query = "MERGE (m:Address {address: '" + inputAddr + "'}) MERGE (n:Address {address:'" + outputAddr + "'}) MERGE (m)-[:PAY {amount:'"
						    + weightedOutputValue.toPlainString() + "', weight: '" + ratioEntry.getValue().toPlainString() + "', time:" + blockTx.getTime() + ", txHash:'"+blockTx.getHash()+"', blockHeight: "+block.getHeight()+", blockHash:'"+block.getHash()+"'}]->(n)";
					    System.out.println(query);
					    tx.run(query);

					}
				    }

				}
			    }

			    return null;
			}
		    });

		}

	    }

	}
    }
}
