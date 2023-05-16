import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider, ProfileCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

object AWS {
      /* 
     * This function is used to build an S3 client. It will first try to use the
     * credentials in the default profile. If that fails, it will fall back to
     * the default credentials provider.
     */
    // Build S3 client
    def getAWSCredentials: AwsCredentialsProvider = {
        val credentialsProvider = ProfileCredentialsProvider.create()
        if (credentialsProvider.resolveCredentials().accessKeyId() == null) {
            // Fallback to DefaultCredentialsProvider if profile not found or no credentials
            DefaultCredentialsProvider.create()
        } else {
            credentialsProvider
        }
    }

    // Build S3 client
    val awsCredentialsProvider: AwsCredentialsProvider = getAWSCredentials
    val s3Client: S3Client = S3Client.builder()
        .region(Region.US_EAST_1)
        .credentialsProvider(awsCredentialsProvider)
        .build()
    
    s3Client
}
