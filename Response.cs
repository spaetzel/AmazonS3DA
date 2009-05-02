// This software code is made available "AS IS" without warranties of any        
// kind.  You may copy, display, modify and redistribute the software            
// code either by itself or as incorporated into your code; provided that        
// you do not remove any proprietary notices.  Your use of this software         
// code is at your own risk and you waive any claim against Amazon               
// Digital Services, Inc. or its affiliates with respect to your use of          
// this software code. (c) 2006 Amazon Digital Services, Inc. or its             
// affiliates.          



using System;
using System.Net;
using System.Text;

namespace com.CastRoller.AmazonS3DA
{
    public class Response
    {
        protected WebResponse response;
        public WebResponse Connection
        {
            get
            {
                return response;
            }
        }

        public HttpStatusCode Status
        {
            get
            {
                HttpWebResponse wr = response as HttpWebResponse;
                return wr.StatusCode;
            }
        }

        public string XAmzId
        {
            get
            {
                return response.Headers.Get( "x-amz-id-2" );
            }
        }

        public string XAmzRequestId
        {
            get
            {
                return response.Headers.Get("x-amz-request-id");
            }
        }

        public Response(WebRequest request)
        {
            try
            {
                this.response = request.GetResponse();
            }
            catch (WebException ex)
            {
                string msg = ex.Response != null ? Utils.slurpInputStreamAsString(ex.Response.GetResponseStream()) : ex.Message;
                throw new WebException(msg, ex, ex.Status, ex.Response);
            }
        }

        public byte [] getResponseMessage()
        {
            byte[] data = Utils.slurpInputStream( response.GetResponseStream() );
            response.GetResponseStream().Close();
            return data;
        }
    }
}
