// This software code is made available "AS IS" without warranties of any        
// kind.  You may copy, display, modify and redistribute the software            
// code either by itself or as incorporated into your code; provided that        
// you do not remove any proprietary notices.  Your use of this software         
// code is at your own risk and you waive any claim against Amazon               
// Digital Services, Inc. or its affiliates with respect to your use of          
// this software code. (c) 2006 Amazon Digital Services, Inc. or its             
// affiliates.          



using System;
using System.Collections;
using System.Text;

namespace com.CastRoller.AmazonS3DA
{
    public class S3Object
    {
        private byte [] bytes;

        /// <summary>
        /// Acquires the binary representation of an object.
        /// </summary>
        public byte [] Bytes {
            get {
                return bytes;
            }
        }

        /// <summary>
        /// Acquires the ASCII Encoding representation of an object.
        /// </summary>
        public string Data {
            get {
                ASCIIEncoding encoder = new ASCIIEncoding();
                return encoder.GetString(bytes, 0, bytes.Length);
            }
        }

        private SortedList metadata;

        /// <summary>
        /// Acquires the metadata.
        /// </summary>
        public SortedList Metadata {
            get {
                return metadata;
            }
        }

        /// <summary>
        /// Constructs an S3Object.
        /// </summary>
        /// <param name="bytes">Byte array representing the object</param>
        /// <param name="metadata">Metadata associated with the object</param>
        public S3Object( byte [] bytes, SortedList metadata ) {
            this.bytes = bytes;
            this.metadata = metadata;
        }

        /// <summary>
        /// Constructs an S3Object.
        /// </summary>
        /// <param name="data">String representation of the data; this will be decoded via ASCII</param>
        /// <param name="metadata">Metadata associated with the object</param>
        public S3Object(string data, SortedList metadata)
        {
            ASCIIEncoding encoder = new ASCIIEncoding();
            this.bytes = encoder.GetBytes( data.ToCharArray() );
            this.metadata = metadata;
        }
    }
}
