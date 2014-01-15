using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.IO;
using System.Text;

namespace com.bitscopic.downstream.domain
{
    public class ResponseReader : Stream
    {
        public String JsonpCallback { get; set; }
        public Int64 JsonpResponseLength { get; set; }
        Stream _stream;
        string _responseString;

        public ResponseReader(Stream stream)
        {
            _stream = stream;
        }

        /// <summary>
        /// This custom filter caches the response value in this accessor
        /// </summary>
        public string ResponseString
        {
            get { return _responseString; }
            set { _responseString = value; }
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
            System.Console.WriteLine("flush called");
            _stream.Flush();
        }

        public override long Length
        {
            get { return _stream.Length; }
        }

        public override long Position
        {
            get
            {
                return _stream.Position;
            }
            set
            {
                _stream.Position = value;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _stream.Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _stream.SetLength(value);
        }

        StringBuilder _responseSB = new StringBuilder();

        /// <summary>
        /// The overridden write converts the output to text and caches it in the ResponseString accessor. It
        /// finally converts that string back to an array of bytes and writes it back out to the wrapped stream
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            System.Console.WriteLine("write called");
            _stream.Write(buffer, offset, count);
            return;
            // if we found the request was a jsonp request
            if (!String.IsNullOrEmpty(this.JsonpCallback))
            {
                if (this.JsonpResponseLength == 0) // first time through - create beginning of JSONP callback
                {
                    String jsonpCallBackBeginning = String.Concat(this.JsonpCallback, "(");
                    _responseSB.Append(jsonpCallBackBeginning);
                    _responseSB.Append(System.Text.Encoding.Default.GetString(buffer, offset, count));
                    this.JsonpResponseLength = _responseSB.Length;
                }
                this.JsonpResponseLength += _responseString.Length;

                _stream.Write(System.Text.Encoding.Default.GetBytes(_responseSB.ToString()), 0, count);
            }
            else
            {
                _stream.Write(buffer, offset, count);
            }
            byte[] b = System.Text.Encoding.Default.GetBytes(_responseString);
        }
    }
}