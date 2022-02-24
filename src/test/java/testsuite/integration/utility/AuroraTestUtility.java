/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package testsuite.integration.utility;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.CreateDBClusterRequest;
import com.amazonaws.services.rds.model.CreateDBInstanceRequest;
import com.amazonaws.services.rds.model.DeleteDBClusterRequest;
import com.amazonaws.services.rds.model.DeleteDBInstanceRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;
import com.amazonaws.services.rds.model.Filter;
import com.amazonaws.services.rds.model.Tag;
import com.amazonaws.services.rds.waiters.AmazonRDSWaiters;
import com.amazonaws.waiters.NoOpWaiterHandler;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;
import com.amazonaws.waiters.WaiterTimedOutException;
import com.amazonaws.waiters.WaiterUnrecoverableException;
import com.mysql.cj.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Creates and destroys AWS RDS Cluster and Instances
 * AWS Credentials is loaded using DefaultAWSCredentialsProviderChain
 *      Environment Variable > System Properties > Web Identity Token > Profile Credentials > EC2 Container
 * To specify which to credential provider, use AuroraTestUtility(String region, AWSCredentialsProvider credentials) *
 *
 * If using environment variables for credential provider
 *     Required
 *     - AWS_ACCESS_KEY_ID
 *     - AWS_SECRET_ACCESS_KEY
 */
public class AuroraTestUtility {
    // Default values
    private String dbUsername = "my_test_username";
    private String dbPassword = "my_test_password";
    private String dbName = "test";
    private String dbIdentifier = "test-identifier";
    private String dbEngine = "aurora-mysql";
    private String dbInstanceClass = "db.r5.large";
    private final String dbRegion;
    private final String dbSecGroup = "default";
    private int numOfInstances = 5;

    private final AmazonRDS rdsClient;
    private final AmazonEC2 ec2Client;

    private static final String DUPLICATE_IP_ERROR_CODE = "InvalidPermission.Duplicate";

    /**
     * Initializes an AmazonRDS & AmazonEC2 client.
     * RDS client used to create/destroy clusters & instances.
     * EC2 client used to add/remove IP from security group.
     */
    public AuroraTestUtility() {
        this("us-east-1", new DefaultAWSCredentialsProviderChain());
    }

    /**
     * Initializes an AmazonRDS & AmazonEC2 client.
     * @param region define AWS Regions, refer to https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
     */
    public AuroraTestUtility(String region) {
        this(region, new DefaultAWSCredentialsProviderChain());
    }

    /**
     * Initializes an AmazonRDS & AmazonEC2 client.
     * @param region define AWS Regions, refer to https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
     * @param credentials Specific AWS credential provider
     */
    public AuroraTestUtility(String region, AWSCredentialsProvider credentials) {
        dbRegion = region;

        rdsClient = AmazonRDSClientBuilder
            .standard()
            .withRegion(dbRegion)
            .withCredentials(credentials)
            .build();

         ec2Client = AmazonEC2ClientBuilder
            .standard()
            .withRegion(dbRegion)
            .withCredentials(credentials)
            .build();
    }

    /**
     * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
     * @param username Master username for access to database
     * @param password Master password for access to database
     * @param name Database name
     * @param identifier Database cluster identifier
     * @param engine Database engine to use, refer to https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Welcome.html
     * @param instanceClass instance class, refer to https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.html
     * @param instances number of instances to spin up
     * @return An endpoint for one of the instances
     * @throws InterruptedException when clusters have not started after 30 minutes
     */
    public String createCluster(String username, String password, String name, String identifier, String engine, String instanceClass, int instances)
        throws InterruptedException {
        dbUsername = username;
        dbPassword = password;
        dbName = name;
        dbIdentifier = identifier;
        dbEngine = engine;
        dbInstanceClass = instanceClass;
        numOfInstances = instances;
        return createCluster();
    }

    /**
     * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
     * @param username Master username for access to database
     * @param password Master password for access to database
     * @param name Database name
     * @param identifier Database identifier
     * @return An endpoint for one of the instances
     * @throws InterruptedException when clusters have not started after 30 minutes
     */
    public String createCluster(String username, String password, String name, String identifier)
        throws InterruptedException {
        dbUsername = username;
        dbPassword = password;
        dbName = name;
        dbIdentifier = identifier;
        return createCluster();
    }

    /**
     * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
     * @return An endpoint for one of the instances
     * @throws InterruptedException when clusters have not started after 30 minutes
     */
    public String createCluster() throws InterruptedException {
        // Create Cluster
        final Tag testRunnerTag = new Tag()
            .withKey("env")
            .withValue("test-runner");
        final CreateDBClusterRequest dbClusterRequest = new CreateDBClusterRequest()
            .withDBClusterIdentifier(dbIdentifier)
            .withDatabaseName(dbName)
            .withMasterUsername(dbUsername)
            .withMasterUserPassword(dbPassword)
            .withSourceRegion(dbRegion)
            .withEnableIAMDatabaseAuthentication(true)
            .withEngine(dbEngine)
            .withStorageEncrypted(true)
            .withTags(testRunnerTag);

        rdsClient.createDBCluster(dbClusterRequest);

        // Create Instances
        final CreateDBInstanceRequest dbInstanceRequest = new CreateDBInstanceRequest()
            .withDBClusterIdentifier(dbIdentifier)
            .withDBInstanceClass(dbInstanceClass)
            .withEngine(dbEngine)
            .withPubliclyAccessible(true)
            .withTags(testRunnerTag);

        for (int i = 1; i <= numOfInstances; i++) {
            rdsClient.createDBInstance(dbInstanceRequest.withDBInstanceIdentifier(dbIdentifier + "-" + i));
        }

        // Wait for all instances to be up
        final AmazonRDSWaiters waiter = new AmazonRDSWaiters(rdsClient);
        final DescribeDBInstancesRequest dbInstancesRequest = new DescribeDBInstancesRequest()
            .withFilters(new Filter().withName("db-cluster-id").withValues(dbIdentifier));
        final Waiter<DescribeDBInstancesRequest> instancesRequestWaiter = waiter
            .dBInstanceAvailable();
        final Future<Void> future = instancesRequestWaiter.runAsync(new WaiterParameters<>(dbInstancesRequest), new NoOpWaiterHandler());
        try {
            future.get(30, TimeUnit.MINUTES);
        } catch (WaiterUnrecoverableException | WaiterTimedOutException | TimeoutException | ExecutionException exception) {
            deleteCluster();
            throw new InterruptedException("Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
        }

        final DescribeDBInstancesResult dbInstancesResult = rdsClient.describeDBInstances(dbInstancesRequest);
        final String endpoint = dbInstancesResult.getDBInstances().get(0).getEndpoint().getAddress();
        return endpoint.substring(endpoint.indexOf('.') + 1);
    }

    /**
     * Gets public IP
     * @return public IP of user
     * @throws UnknownHostException
     */
    public String getPublicIPAddress() throws UnknownHostException {
        String ip;
        try {
            URL ipChecker = new URL("http://checkip.amazonaws.com");
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                ipChecker.openStream()));
            ip = reader.readLine();
        } catch (Exception e) {
            throw new UnknownHostException("Unable to get IP");
        }
        return ip;
    }

    /**
     * Authorizes IP to EC2 Security groups for RDS access.
     */
    public void ec2AuthorizeIP(String ipAddress) {
        if (StringUtils.isNullOrEmpty(ipAddress)) {
            return;
        }
        final AuthorizeSecurityGroupIngressRequest authRequest = new AuthorizeSecurityGroupIngressRequest()
            .withGroupName(dbSecGroup)
            .withCidrIp(ipAddress + "/32")
            .withIpProtocol("-1") // All protocols
            .withFromPort(0) // For all ports
            .withToPort(65535);

        try {
            ec2Client.authorizeSecurityGroupIngress(authRequest);
        } catch (AmazonEC2Exception exception) {
            if (!DUPLICATE_IP_ERROR_CODE.equalsIgnoreCase(exception.getErrorCode())) {
                throw exception;
            }
        }
    }

    /**
     * De-authorizes IP from EC2 Security groups.
     * @throws UnknownHostException
     */
    public void ec2DeauthorizesIP(String ipAddress) {
        if (StringUtils.isNullOrEmpty(ipAddress)) {
            return;
        }
        final RevokeSecurityGroupIngressRequest revokeRequest = new RevokeSecurityGroupIngressRequest()
            .withGroupName(dbSecGroup)
            .withCidrIp(ipAddress + "/32")
            .withIpProtocol("-1") // All protocols
            .withFromPort(0) // For all ports
            .withToPort(65535);

        try {
            ec2Client.revokeSecurityGroupIngress(revokeRequest);
        } catch (AmazonEC2Exception exception) {
            // Ignore
        }
    }

    /**
     * Destroys all instances and clusters. Removes IP from EC2 whitelist.
     * @param identifier database identifier to delete
     */
    public void deleteCluster(String identifier) {
        dbIdentifier = identifier;
        deleteCluster();
    }

    /**
     * Destroys all instances and clusters. Removes IP from EC2 whitelist.
     */
    public void deleteCluster() {
        // Tear down instances
        final DeleteDBInstanceRequest dbDeleteInstanceRequest = new DeleteDBInstanceRequest()
            .withSkipFinalSnapshot(true);

        for (int i = 1; i <= numOfInstances; i++) {
            rdsClient.deleteDBInstance(dbDeleteInstanceRequest.withDBInstanceIdentifier(dbIdentifier + "-" + i));
        }

        // Tear down cluster
        final DeleteDBClusterRequest dbDeleteClusterRequest = new DeleteDBClusterRequest()
            .withSkipFinalSnapshot(true)
            .withDBClusterIdentifier(dbIdentifier);

        rdsClient.deleteDBCluster(dbDeleteClusterRequest);
    }
}
