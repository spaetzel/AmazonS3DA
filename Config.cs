using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.CastRoller.AmazonS3DA
{
    public static class Config
    {
        private static string _awsAccessKey;

        public static string AwsAccessKey
        {
            get { return _awsAccessKey; }
        }
        private static string _awsSecretKey;

        public static string AwsSecretKey
        {
            get { return _awsSecretKey; }
        }

        public static void SetConfigurations(string awsAccessKey, string awsSecretKey)
        {
            _awsSecretKey = awsSecretKey;
            _awsAccessKey = awsAccessKey;
        }
    }
}
