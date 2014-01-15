using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.reporting
{
    [Serializable]
    public class Report
    {
        public Report(String id)
        {
            this.BatchId = id;
            _startTimestamp = DateTime.Now;
        }

        public String BatchId { get; set; }

        DateTime _startTimestamp;
        public DateTime StartTimestamp { get { return _startTimestamp; } }

        DateTime _endTimestamp;
        public DateTime EndTimestamp { get { return _endTimestamp; } set { _endTimestamp = value; } }

        IList<Exception> _exceptions = new List<Exception>();
        /// <summary>
        /// Get a list of this class' current exceptions
        /// </summary>
        public IList<Exception> Exceptions { get { return _exceptions; } }

        IList<string> _infoMessages = new List<string>();
        /// <summary>
        /// Get a list of this class' current info messages
        /// </summary>
        public IList<string> InfoMessages { get { return _infoMessages; } }

        IList<string> _errorMessages = new List<string>();
        /// <summary>
        /// Get a list of this class' current error messages
        /// </summary>
        public IList<string> ErrorMessages { get { return _errorMessages; } }

        IList<string> _debugMessages = new List<string>();
        /// <summary>
        /// Get a list of this class' current debug messages
        /// </summary>
        public IList<string> DebugMessages { get { return _debugMessages; } }

        //private readonly object _lock = new object();

        /// <summary>
        /// Add an exception object to this class' exception list
        /// </summary>
        /// <param name="exc">Exception object</param>
        public void addException(Exception exc)
        {
               _exceptions.Add(exc);
        }

        /// <summary>
        /// Add an error message to this class' error message list
        /// </summary>
        /// <param name="message">The message</param>
        public void addError(string message)
        {
            _errorMessages.Add(DateTime.Now + ":" + message + "\n");
        }

        public void addError(string message, Exception exc)
        {
            if (exc == null)
            {
                addError(message);
            }
            else
            {
                _errorMessages.Add(message + Environment.NewLine + "Exception: " + exc.ToString());
            }
        }

        /// <summary>
        /// Add an info message to this class' info message list
        /// </summary>
        /// <param name="message">The message</param>
        public void addInfo(string message)
        {
            _infoMessages.Add(DateTime.Now + ":" + message + "\n");
        }

        /// <summary>
        /// Add a debug message to this class' debug message list
        /// </summary>
        /// <param name="message">The message</param>
        public void addDebug(string message)
        {
              _debugMessages.Add(DateTime.Now + ":" + message + "\n");
        }

        /// <summary>
        /// Build a string out of object's properties
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Software Version: " + System.Reflection.Assembly.GetExecutingAssembly().FullName);
            sb.AppendLine("Info Messages:");
            if (InfoMessages == null || InfoMessages.Count == 0)
            {
                sb.AppendLine("*** No info messages! ***");
            }
            else
            {
                foreach (string s in InfoMessages)
                {
                    sb.AppendLine(s);
                }
            }

            sb.AppendLine("Error Messages:");
            if (ErrorMessages == null || ErrorMessages.Count == 0)
            {
                sb.AppendLine("*** No error messages! ***");
            }
            else
            {
                foreach (string s in ErrorMessages)
                {
                    sb.AppendLine(s);
                }
            }

            sb.AppendLine("Debug Messages:");
            if (DebugMessages == null || DebugMessages.Count == 0)
            {
                sb.AppendLine("*** No debug messages! ***");
            }
            else
            {
                foreach (string s in DebugMessages)
                {
                    sb.AppendLine(s);
                }
            }

            sb.AppendLine("Exceptions:");
            if (Exceptions == null || Exceptions.Count == 0)
            {
                sb.AppendLine("*** No exception messages! ***");
            }
            else
            {
                foreach (Exception e in Exceptions)
                {
                    sb.AppendLine(e.ToString());
                }
            }
            return sb.ToString();
        }

        public void clear()
        {
            
            _startTimestamp = DateTime.Now;
            _exceptions = new List<Exception>();
            _infoMessages = new List<string>();
            _errorMessages = new List<string>();
            _debugMessages = new List<string>();
        }
    }
}
