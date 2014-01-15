using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Threading;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Configuration;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.net.utils;
using System.Net.NetworkInformation;
using System.ComponentModel;
using com.bitscopic.downstream.logging;

namespace com.bitscopic.downstream.net
{
    public class SocketContainer
    {
        public ManualResetEvent AllDone { get; set; }
        public Socket MainSocket { get; set; }
        public bool Locked { get; set; }
        public bool Listening { get; set; }
        public string HostName { get; set; }
        public Int32 ListeningPort { get; set; }
        public ServiceState ServiceState { get; set; }

        public SocketContainer()
        {
            this.ServiceState = new ServiceState();
            this.AllDone = new ManualResetEvent(false);
        }
    }

    public class Server
    {
        //static byte EOT = System.Text.Encoding.ASCII.GetBytes("\x04")[0];

        public SocketContainer SocketContainer { get; set; }
       // public Dictionary<String, Object> SharedData { get; set; }

        public Server()
        {
            this.SocketContainer = new SocketContainer();
            setHostName(this.SocketContainer);
            Int32 listeningPort = 2702; // default
            Int32.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.ServerPort], out listeningPort); // if this fails, default is fine
            this.SocketContainer.ListeningPort = listeningPort;
        }

        public Server(String hostName, Int32 listeningPort)
        {
            this.SocketContainer = new SocketContainer();
            this.SocketContainer.HostName = hostName;
            this.SocketContainer.ListeningPort = listeningPort;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="randomPort"></param>
        public Server(bool randomPort)
        {
            this.SocketContainer = new SocketContainer();
            setHostName(this.SocketContainer);

            if (randomPort)
            {
                bool foundOpenPort = false;
                this.SocketContainer.ListeningPort = PortNumberGenerator.getRandomPort();
                while (!foundOpenPort)
                {
                    try
                    {
                        Client c = new Client();
                        c.connect(this.SocketContainer.HostName, this.SocketContainer.ListeningPort);
                        c.disconnect();
                        this.SocketContainer.ListeningPort = PortNumberGenerator.getRandomPort();
                    }
                    catch (Exception)
                    {
                        foundOpenPort = true;
                    }
                }
            }
            else
            {
                this.SocketContainer.ListeningPort = 2702; // use as default
                Int32 listeningPort = 2702; // default
                Int32.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.ServerPort], out listeningPort); // if this fails, default is fine
                this.SocketContainer.ListeningPort = listeningPort;
            }
        }

        void setHostName(SocketContainer srv)
        {
            String configHostName = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.ServerHostName];
            if (!String.IsNullOrEmpty(configHostName))
            {
                srv.HostName = configHostName;
            }
            else
            {
                srv.HostName = IPv4Helper.getIPv4Address().ToString(); // want IP address for hostname
            }
        }

        /// <summary>
        /// This is the function signature for using the System.ComponentModel.BackgroundWorker API
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public void startListener(object sender, DoWorkEventArgs e)
        {
            startListenerBlocking();
        }

        /// <summary>
        /// Place the server in to a listening state
        /// </summary>
        public void startListener()
        {
            Thread t = new Thread(new ThreadStart(startListenerBlocking));
            t.Name = "Server";
            t.Start();
        }

        void startListenerBlocking()
        {
            try
            {
                this.SocketContainer.MainSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                this.SocketContainer.MainSocket.Bind(IPv4Helper.createIPv4Endpoint(this.SocketContainer.ListeningPort));
                this.SocketContainer.MainSocket.Listen(256);

                this.SocketContainer.MainSocket.BeginAccept(new AsyncCallback(acceptCallback), this.SocketContainer);
                this.SocketContainer.Listening = true;

                while (this.SocketContainer.Listening)
                {
                    this.SocketContainer.AllDone.Reset();
                    this.SocketContainer.AllDone.WaitOne();
                }
                try
                {
                    //this.SocketContainer.MainSocket.Shutdown(SocketShutdown.Receive);
                    this.SocketContainer.MainSocket.Close();
                }
                catch (Exception)
                {
                    // just swallow
                    //System.Console.WriteLine("Problem shutting down listener: " + exc.ToString());
                }
            }
            catch (Exception)
            {
                // try again?
                //System.Threading.Thread.Sleep(1000);
                //System.Diagnostics.StackTrace st = new System.Diagnostics.StackTrace();
                //System.Console.WriteLine(st.ToString());
                throw;
            }

        }

         /// <summary>
         /// Stop the server from listening for connections
         /// </summary>
        public void stopListener()
        {
            this.SocketContainer.Listening = false;
            this.SocketContainer.AllDone.Set();
            if (this.SocketContainer.MainSocket != null && this.SocketContainer.ServiceState != null)
            {
                this.SocketContainer.ServiceState.Status = ServiceStatus.STOPPED;
            }
        }

        void acceptCallback(IAsyncResult iar)
        {
            SocketContainer srv = (SocketContainer)iar.AsyncState;
            if (!srv.Listening)
            {
                return;
            }
            Socket worker = null;
            try
            {
                // free up the main socket to accept requests immediately and assign socket to worker
                worker = srv.MainSocket.EndAccept(iar);
                // place main socket back in listening state for more connections
                this.SocketContainer.MainSocket.BeginAccept(new AsyncCallback(acceptCallback), this.SocketContainer);
            }
            catch
            {
                try
                {
                    // wait and try again
                    Thread.Sleep(100);
                    // free up the main socket to accept requests immediately and assign socket to worker
                    worker = this.SocketContainer.MainSocket.EndAccept(iar);
                    // place main socket back in listening state for more connections
                    this.SocketContainer.MainSocket.BeginAccept(new AsyncCallback(acceptCallback), this.SocketContainer);
                }
                catch { return; } // TODO: add thread safe logging to the reqhandle and log the beginaccept disposal
            }
            // copy reference to service state on our threadsafestateobject so shutdown can still be signaled
            ThreadSafeStateObject state = new ThreadSafeStateObject(worker, new byte[ThreadSafeStateObject.BufferSize]) { ServiceState = srv.ServiceState };

            srv.AllDone.Set();
            worker.BeginReceive(state.Buffer, 0, ThreadSafeStateObject.BufferSize, SocketFlags.None, new AsyncCallback(dataReceivedCallback), state);
        }

        //void waitForData(StateObject state)
        //{
        //    state.Socket.BeginReceive(state.Buffer, 0, StateObject.BufferSize, SocketFlags.None, new AsyncCallback(dataReceivedCallback), state);
        //}

        void dataReceivedCallback(IAsyncResult iar)
        {
            ThreadSafeStateObject state = (ThreadSafeStateObject)iar.AsyncState;
            Socket worker = state.Socket;
            try
            {
                int bytesRead = worker.EndReceive(iar);
                if (bytesRead == 0 && state.getMemoryStreamLength() == 0)
                {
                    worker.Close();
                    return;
                }

                // first get length header
                int msgLength = 0;
                for (int i = 0; i < bytesRead; i++)
                {
                    if (state.Buffer[i] == '\x7c')
                    {
                        msgLength = Convert.ToInt32(System.Text.ASCIIEncoding.ASCII.GetString(state.Buffer.Take(i).ToArray()));
                        bytesRead = bytesRead - i - 1; // -1 for the pipe too
                        break;
                    }
                }

                state.Buffer = state.Buffer.Skip(msgLength.ToString().Length + 1).ToArray(); // now reset chunk to skip the header

                state.addBytesToMemoryStream(state.Buffer, bytesRead);
                state.Buffer = new byte[ThreadSafeStateObject.BufferSize];
                while (state.getMemoryStreamLength() < msgLength) // while not EOT
                {
                    bytesRead = worker.Receive(state.Buffer);
                    state.addBytesToMemoryStream(state.Buffer, bytesRead);
                    state.Buffer = new byte[ThreadSafeStateObject.BufferSize];
                }
                //state.IsEOT = false;

                BinaryFormatter bf = new BinaryFormatter();
                //state.BytesInMemory.Position = 0; // set position in stream to zero so deserialization succeeds
                byte[] bytes = state.getMemoryStream();
                Stream memStream = new MemoryStream(bytes);
                memStream.Position = 0;
                MessageTO messageTO = (MessageTO)(bf.Deserialize(memStream));

                sendToClient(state, RequestHandler.getInstance().handleRequest(messageTO, this)); // pass this because some methods use the properties and requesthandler is a singleton and not unique among threads

                // this is our hook for thread safety mentioned in RequestHandler
                // before returning check to see if shutdown signal was set
                if (messageTO.MessageType == MessageTypeTO.StopServerRequest) // this.ServiceState != null && this.ServiceState.Status == ServiceStatus.STOPPED)
                {
                    if (state.ServiceState != null)
                    {
                        state.ServiceState.Status = ServiceStatus.STOPPED;
                    }
                    stopListener();
                }
            }
            catch (Exception)
            {
                //LOG.Error("An unrecoverable error occurred while sending/receiving data", exc);
                try
                {
                    worker.Shutdown(SocketShutdown.Both);
                    worker.Close();
                }
                catch (Exception) { }
            }
        }

        void sendAckToClient(ThreadSafeStateObject state, string messageSize)
        {
            byte[] buffer = System.Text.Encoding.ASCII.GetBytes(messageSize);
            NetworkStream stream = new NetworkStream(state.Socket);
            stream.Write(buffer, 0, buffer.Length);
            stream.Flush();
        }

        void sendToClient(ThreadSafeStateObject state, string message)
        {
            MessageTO messageObject = new MessageTO() { Message = message };
            sendToClient(state, messageObject);
        }

        void sendToClient(ThreadSafeStateObject state, MessageTO message)
        {
            BinaryFormatter serializer = new BinaryFormatter();
            MemoryStream bytesToSend = new MemoryStream();
            serializer.Serialize(bytesToSend, message);

            //bytesToSend.WriteByte(EOT); // add EOT before setting stream position
            bytesToSend.Position = 0;
            NetworkStream stream = new NetworkStream(state.Socket);
            byte[] header = System.Text.ASCIIEncoding.ASCII.GetBytes(String.Concat(bytesToSend.Length.ToString() + "|"));
            stream.Write(header, 0, header.Length);
            stream.Write(bytesToSend.GetBuffer(), 0, (Int32)bytesToSend.Length);
            stream.Flush();
            try
            {
                state.Socket.Shutdown(SocketShutdown.Both);
                state.Socket.Close();
            }
            catch (Exception exc)
            {
                //LOG.Error("An exception was caught trying to shut down a background server socket", exc);
            }
        }
    }

    public class ThreadSafeStateObject
    {
        //static byte EOT = System.Text.Encoding.ASCII.GetBytes("\x04")[0];
        //public bool IsEOT { get; set; }
        public Int32 MessageSize { get; set; }
        public byte[] Buffer { get; set; }
        public Socket Socket { get; set; }
        public static int BufferSize { get { return _bufferSize; } }
        const int _bufferSize = 8192; // TBD - better way to handle this? Don't like hard coded buffer size
        public ServiceState ServiceState { get; set; }
 
        private static readonly object _locker = new object(); 
        private MemoryStream _bytesInMemory = new MemoryStream();

        public ThreadSafeStateObject(Socket socket, byte[] buffer) 
        {
            Socket = socket;
            Buffer = buffer;
        }

        public void addBytesToMemoryStream(byte[] bytes, int length)
        {
            lock (_locker)
            {
                _bytesInMemory.Write(bytes, 0, length);
                //if (bytes[length - 1] == EOT)
                //{
                //    this.IsEOT = true;
                //    _bytesInMemory.Write(bytes, 0, length - 1);
                //}
                //else
                //{
                //    _bytesInMemory.Write(bytes, 0, length);
                //}
            }
        }

        public int getMemoryStreamLength()
        {
            lock (_locker)
            {
                return Convert.ToInt32(_bytesInMemory.Length);
            }
        }

        public byte[] getMemoryStream()
        {
            lock (_locker)
            {
                return _bytesInMemory.GetBuffer();
                //_bytesInMemory.Position = 0;
                //byte[] localBuffer = new byte[_bytesInMemory.Length];
                //localBuffer = _bytesInMemory.ToArray();
                //_bytesInMemory = new MemoryStream();
                //return localBuffer;
            }
        }
    }
}
