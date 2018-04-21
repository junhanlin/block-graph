package com.example.blockgraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.esotericsoftware.kryo.Kryo;

import info.blockchain.api.APIException;
import info.blockchain.api.blockexplorer.Block;
import info.blockchain.api.blockexplorer.BlockExplorer;

public class BlockFetcher
{
    private final static long LATEST_BLOCK_HEIGHT = 519152;

    public static void main(String[] args) throws APIException, IOException, InterruptedException, ConfigurationException
    {

	Parameters params = new Parameters();
	// Read data from this file
	File propertiesFile = new File(args[0]);

	FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
		.configure(params.fileBased().setFile(propertiesFile));
	Configuration config = builder.getConfiguration();

	String awsAccountKey = (String) config.getProperty("aws.account.key");
	String awsAccountSecret = (String) config.getProperty("aws.account.secret");
	String bucketName = (String) config.getProperty("aws.s3.bucket");

	AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(awsAccountKey, awsAccountSecret));

	Kryo kryo = new Kryo();
	BlockExplorer blockExplorer = new BlockExplorer();

	long blockHeight = Long.parseLong(args[1]);

	while (blockHeight > 0)
	{
	    System.out.println("Fetching block at height " + blockHeight);
	    List<Block> blocks = blockExplorer.getBlocksAtHeight(blockHeight);

	    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

	    try (com.esotericsoftware.kryo.io.Output kryoOutput = new com.esotericsoftware.kryo.io.Output(byteOut))
	    {
		BlockHeight blockHeightObj = new BlockHeight();
		blockHeightObj.setBlocks(blocks);
		kryo.writeObject(kryoOutput, blockHeightObj);

	    }

	    try (ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray()))
	    {
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, "height-" + blockHeight, byteIn, new ObjectMetadata());

		PutObjectResult putObjectResult = s3Client.putObject(putObjectRequest);

	    }
	    System.out.println("Done!");
	    blockHeight--;
	    Thread.sleep(600);
	}

    }
}
