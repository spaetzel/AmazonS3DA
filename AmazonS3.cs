using System;
using System.IO;
using Spaetzel.UtilityLibrary;

namespace com.CastRoller.AmazonS3DA
{
	/// <summary>
	/// A class that provides simple static methods for performing common operations on Amazon S3
	/// </summary>
	public class AmazonS3
	{
		/// <summary>
		/// Returns an authenticated connection to Amazon S3
		/// </summary>
		/// <returns></returns>
		public static AWSAuthConnection GetConnection()
		{
			return new AWSAuthConnection( Config.AwsAccessKey, Config.AwsSecretKey );
		}

		/// <summary>
		/// Copies a file from the local filesystem to the specified bucket
		/// </summary>
		/// <param name="bucket">The bucket on s3 the file should be coppied to</param>
		/// <param name="key">The name that should be used on s3 for the file</param>
		/// <param name="filePath">The full path to the file on the local filesystem</param>
		/// <returns>An S3 response for the results of putting the file to S3</returns>
		public static Response PutFile( string bucket, string key, string filePath, bool setPublic )
		{
	

			// Open the file for streaming
			Stream dis = File.OpenRead(filePath);

            
            

            return PutStream(bucket, key, dis, setPublic);
		}

        public static Response PutStream(string bucket, string key, Stream stream, bool setPublic )
        {
            S3StreamObject s3Object = new S3StreamObject(stream, null);

            int lastSlash = Math.Max(key.LastIndexOf("/"), key.LastIndexOf("\\"));


            string fileName = key.Substring(lastSlash + 1);

            string extension = fileName.Substring(fileName.LastIndexOf(".") + 1).ToLower();


            System.Collections.SortedList headers = new System.Collections.SortedList();

            bool setDisposition = false;

            if (extension == "exe" || extension == "bin")
            {
                headers["Content-Type"] = "application/octet-stream";
                setDisposition = true;
            }
            else if (extension == "zip")
            {
                headers["Content-Type"] = "application/zip";
                setDisposition = true;
            }
            else if (extension == "tar")
            {
                headers["Content-Type"] = "application/x-tar";
                setDisposition = true;
            }
            else if (extension == "gz")
            {
                headers["Content-Type"] = "application/x-gzip";
                setDisposition = true;
            }
            else if (extension == "png")
            {
                headers["Content-Type"] = "image/png";
            }
            else if (extension == "gif")
            {
                headers["Content-Type"] = "image/gif";
            }
            else if (extension == "jpg" || extension == "jpeg" )
            {
                headers["Content-Type"] = "image/jpeg";
         
            }

            if (setDisposition)
            {
                headers["Content-Disposition"] = "attachment; filename=\"" + fileName + "\"";
            }

            try
            {


                // Get a connection to s3
                AWSAuthConnection conn = AmazonS3.GetConnection();


                // Stream the file to S3
                Response response = conn.putStream(bucket, key, s3Object, headers);

                if (setPublic)
                {
                    string acl = "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner><ID>673171425ac8d62a827ebfd26c1e51c0dd70016e822369e964467887ee3a0e7d</ID><DisplayName>amazon</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>673171425ac8d62a827ebfd26c1e51c0dd70016e822369e964467887ee3a0e7d</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>";
                    conn.putACL(bucket, key, acl, null);
                }

                return response;
            }
            catch
            {
                // Probably couldn't connect ro s3
                return null;
            }
        }

		/// <summary>
		/// Deletes the given file from Amazon S3
		/// </summary>
		/// <param name="bucket">The bucket to delete from</param>
		/// <param name="key">The key to delete</param>
		/// <returns>An S3 response with the results of deleting the file</returns>
		public static Response DeleteFile( string bucket, string key )
		{

			// Get a connection
			AWSAuthConnection conn = AmazonS3.GetConnection();
			
			// Actually delete the file
			Response response = conn.delete(bucket, key, null );

			return response;
		}

		/// <summary>
		/// Generates a query string authenticated URL for accessing the specified file on S3
		/// See here for information on query string authentication: http://docs.amazonwebservices.com/AmazonS3/2006-03-01/S3_QSAuth.html
		/// </summary>
		/// <param name="bucket">The bucket the file is contained in</param>
		/// <param name="key">The key for the file to get a download link to</param>
		/// <param name="expires">The DateTime that the download link should expire</param>
		/// <returns>An absolute URL to download the requested file</returns>
		public static string GetDownloadLink( string bucket, string key, DateTime expires )
		{
			// Set up the generator with the AWS login information
			QueryStringAuthGenerator generator = new QueryStringAuthGenerator( Config.AwsAccessKey, Config.AwsSecretKey, false );
			
			// Set the expiry date (Unix timestamp in milliseconds )
			generator.Expires = (long)Utilities.ConvertToUnixTimestamp( expires )*1000;

			// Get the download link
			string link = generator.get( bucket, key, null );

			// If the bucket is a subdomain of castroller.com, change the URL to point to
			// That subdomain instead of amazon
			if( bucket.IndexOf( "castroller.com" ) > 0 )
			{
				string remove = "s3.amazonaws.com:80/";
				link = link.Remove( link.IndexOf(remove), remove.Length );
			}

			return link;
			
		}
	}
}
