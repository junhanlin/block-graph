package com.example.blockgraph;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
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

public class GraphExport
{
    public static void main(String[] args) throws IOException, ConfigurationException
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
	    writer.println("address");
	    final Driver driver = GraphDatabase.driver(neo4jBolt, AuthTokens.basic(neo4jUser, neo4jPassword));
	    try (Session session = driver.session())
	    {
		StatementResult stmtRes = session.run("MATCH (n:Address) RETURN n");
		while (stmtRes.hasNext())
		{
		    Record rec = stmtRes.next();
		    Map<String, Object> propMap = rec.get("n").asNode().asMap();
		    writer.println(propMap.get("address"));

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
		    writer.print(","+rec.get("n").asNode().asMap().get("address"));
		    writer.print(","+propMap.get("amount"));
		    writer.print(","+propMap.get("weight"));
		    writer.print(","+propMap.get("time"));
		    writer.print(","+propMap.get("blockHeight"));
		    writer.print(","+propMap.get("txHash"));
		    writer.println();

		}
	    }
	}

    }

}
