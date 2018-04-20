package com.example.blockgraph;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.esotericsoftware.kryo.Kryo;

import info.blockchain.api.APIException;
import info.blockchain.api.blockexplorer.Address;
import info.blockchain.api.blockexplorer.Block;
import info.blockchain.api.blockexplorer.BlockExplorer;
import info.blockchain.api.blockexplorer.Input;
import info.blockchain.api.blockexplorer.Output;
import info.blockchain.api.blockexplorer.SimpleBlock;
import info.blockchain.api.blockexplorer.Transaction;

public class BlockFetcher
{
    private final static long LATEST_BLOCK_HEIGHT = 519152;

    public static void main(String[] args) throws APIException, IOException, InterruptedException
    {

	File blockDir = new File("/Users/johnlin/Documents/Blocks");
	
	Kryo kryo = new Kryo();
	BlockExplorer blockExplorer = new BlockExplorer();

	
	long blockHeight = LATEST_BLOCK_HEIGHT;
	if(args.length >= 1)
	{
	    blockHeight = Long.parseLong(args[0]);
	}
	
	List<Block> blocks = blockExplorer.getBlocksAtHeight(blockHeight);
	File blockHeightFile = new File(blockDir,"height-"+blockHeight+".part");
	try (com.esotericsoftware.kryo.io.Output kryoOutput = new com.esotericsoftware.kryo.io.Output(new FileOutputStream(blockHeightFile)))
	{
	    BlockHeight blockHeightObj = new BlockHeight();
	    blockHeightObj.setBlocks(blocks);
	    kryo.writeObject(kryoOutput, blockHeightObj);
	}
	
	blockHeightFile.renameTo(new File(blockDir,"height-"+blockHeight));

    }
}
