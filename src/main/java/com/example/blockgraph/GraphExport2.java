package com.example.blockgraph;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;

public class GraphExport2
{
    public static void main(String[] args) throws ConfigurationException, IOException
    {
	Parameters parameters = new Parameters();
	File propertiesFile = new File(args[0]);

	FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
		.configure(parameters.fileBased().setFile(propertiesFile));
	Configuration config = builder.getConfiguration();

	String neo4jBolt = (String) config.getProperty("neo4j.bolt");
	String neo4jUser = (String) config.getProperty("neo4j.user");
	String neo4jPassword = (String) config.getProperty("neo4j.password");

	try (PrintWriter writer = new PrintWriter(new FileWriter(args[1])))
	{
	    writer.println("address,group,category,balance");
	    final Driver driver = GraphDatabase.driver(neo4jBolt, AuthTokens.basic(neo4jUser, neo4jPassword));
	    try (Session session = driver.session())
	    {
		StatementResult stmtRes = session.run("MATCH (n:Address) RETURN n");
		while (stmtRes.hasNext())
		{
		    Record rec = stmtRes.next();
		    Node node = rec.get("n").asNode();
		    Map<String, Object> propMap = node.asMap();
		    String category = "OTHER";
		    if(node.hasLabel("Exchanges"))
		    {
			category = "Exchanges";
		    }
		    else if(node.hasLabel("Gambling"))
		    {
			category = "Gambling";
		    }
		    else if(node.hasLabel("Old"))
		    {
			category = "Old";
		    }
		    else if(node.hasLabel("Pools"))
		    {
			category = "Pools";
		    }
		    else if(node.hasLabel("Services"))
		    {
			category = "Services";
		    }
		    
		    writer.print(propMap.get("address"));
		    writer.print("," + propMap.get("groupName"));
		    writer.print("," + category);
		    writer.print("," + propMap.get("balance"));
		    writer.println();

		}
	    }
	}

	try (PrintWriter writer = new PrintWriter(new FileWriter(args[2])))
	{
	    writer.println("fromAddress,toAddress,amount,weight,time,blockHeight,txHash");
	    final Driver driver = GraphDatabase.driver(neo4jBolt, AuthTokens.basic(neo4jUser, neo4jPassword));
	    try (Session session = driver.session())
	    {
		StatementResult stmtRes = session.run("MATCH (m:Address)-[p:PAY]->(n:Address) RETURN m,p,n");
		while (stmtRes.hasNext())
		{

		    Record rec = stmtRes.next();

		    Map<String, Object> propMap = rec.get("p").asRelationship().asMap();
		    writer.print(rec.get("m").asNode().asMap().get("address"));
		    writer.print("," + rec.get("n").asNode().asMap().get("address"));
		    writer.print("," + propMap.get("amount"));
		    writer.print("," + propMap.get("weight"));
		    writer.print("," + propMap.get("time"));
		    writer.print("," + propMap.get("blockHeight"));
		    writer.print("," + propMap.get("txHash"));
		    writer.println();

		}
	    }
	}
    }
}
