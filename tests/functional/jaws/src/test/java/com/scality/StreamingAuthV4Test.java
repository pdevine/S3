package com.scality;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before ;
import org.junit.BeforeClass ;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Paths;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Bucket;
public class StreamingAuthV4Test {
	protected static String accessKey ;
        public String getAccessKey() { return accessKey ; }
        protected static String secretKey  ;
        public String getSecretKey() { return secretKey; } 
        //run before once before all the tests
	@BeforeClass public static void initAccessKey() throws Exception {
		JSONParser parser = new JSONParser(); 
		String path = Paths.get("config.json").toAbsolutePath().toString();
        	JSONObject obj = (JSONObject) parser.parse(new FileReader(path));
            	StreamingAuthV4Test.accessKey = (String) obj.get("accessKey");
            	StreamingAuthV4Test.secretKey = (String) obj.get("secretKey");
        }

        protected AmazonS3 s3client ;
	public AmazonS3 getS3Client() { return this.s3client; }
        //runs before every test
	@Before public void initAWS3Client() throws Exception {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(getAccessKey(), getSecretKey());
                this.s3client = new AmazonS3Client(awsCreds);
                this.s3client.setEndpoint("http://127.0.0.1:8000");
                this.s3client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
	}
	
 	@Test public void testBucket() throws Exception {
                final String bucketName = "somebucket" ;
 		this.getS3Client().createBucket(bucketName);
  		Object[] buckets=this.getS3Client().listBuckets().toArray();
		Assert.assertEquals(buckets.length,1);
		Bucket bucket = (Bucket) buckets[0];
		Assert.assertEquals(bucketName,bucket.getName());
        }
}
