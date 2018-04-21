package com.example.blockgraph;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import info.blockchain.api.statistics.Statistics;
import software.amazon.ion.Decimal;

public class WalletMetaHelper
{
    private static Map<String, String> cateMap;
    private String address;
    private Configuration config;

    public WalletMetaHelper(Configuration config, String address)
    {
	this.config = config;
	this.address = address;
    }

    public WalletMeta getMeta() throws IOException, SQLException
    {
	WalletMeta retVal = new WalletMeta();
	retVal.setAddress(address);
	Document doc = Jsoup.connect("https://www.walletexplorer.com/address/" + address).validateTLSCertificates(false).get();
	System.out.println("https://www.walletexplorer.com/address/"+address);
	String group = doc.select("#main > h2 > div > a").get(0).text().trim();
	retVal.setGroup(group);
	
	if(group == null)
	{
	    System.out.println();
	}

	if (cateMap == null)
	{
	    cateMap = new HashMap<>();
	    String mysqlJdbcUrl = (String) config.getProperty("mysql.jdbc.url");
	    String mysqlUser = (String) config.getProperty("mysql.jdbc.user");
	    String mysqlPassword = (String) config.getProperty("mysql.jdbc.password");
	    try (Connection conn = DriverManager.getConnection(mysqlJdbcUrl, mysqlUser, mysqlPassword))
	    {
		PreparedStatement outputWalletStmt = conn
			.prepareStatement("SELECT wgc.wallet_group_name,c.name AS category_name FROM wallet_group_category wgc  INNER JOIN category c ON c.id = wgc.category_id");

		ResultSet rs = outputWalletStmt.executeQuery();

		while (rs.next())
		{
		    cateMap.put(rs.getString("wallet_group_name"), rs.getString("category_name"));
		}
	    }

	}
	
	String category = cateMap.get(group);
	retVal.setCategory(category);
	
	String balanceStr = doc.select("#main > table > tbody > tr:nth-child(2) > td:nth-child(3)").get(0).text().trim();
	BigDecimal balance =new BigDecimal(balanceStr).multiply(Decimal.valueOf("100000000"));
	retVal.setBalance(balance.longValue());
	
	return retVal;

    }

    public static class WalletMeta
    {
	private String address;
	private long balance;
	private String category;
	private String group;
	
	

	public String getAddress()
	{
	    return address;
	}

	public void setAddress(String address)
	{
	    this.address = address;
	}

	public long getBalance()
	{
	    return balance;
	}

	public void setBalance(long balance)
	{
	    this.balance = balance;
	}

	public String getCategory()
	{
	    return category;
	}

	public void setCategory(String category)
	{
	    this.category = category;
	}

	public String getGroup()
	{
	    return group;
	}

	public void setGroup(String group)
	{
	    this.group = group;
	}

    }
}
