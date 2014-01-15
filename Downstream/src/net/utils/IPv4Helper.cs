using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Net;

namespace com.bitscopic.downstream.net.utils
{
    public static class IPv4Helper
    {
        public static IPAddress getIPv4AddressForHost(string hostname)
        {
            if (String.IsNullOrEmpty(hostname))
            {
                throw new ApplicationException("Must supply hostname!");
            }
            IPAddress[] addrs = Dns.GetHostAddresses(hostname);
            if (addrs == null || addrs.Length == 0)
            {
                throw new IPv4Exception("No addresses reported from DNS for " + hostname);
            }
            foreach (IPAddress ip in addrs)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip;
                }
            }
            throw new IPv4Exception("Unable to obtain an IPv4 address from DNS for " + hostname);
        }

        public static IPEndPoint createIPv4Endpoint(int endPointPort)
        {
            if (String.IsNullOrEmpty(Dns.GetHostName()))
            {
                throw new IPv4Exception("Unable to obtain local host name!");
            }
            IPAddress[] myIps = Dns.GetHostAddresses(Dns.GetHostName());

            if (myIps == null || myIps.Length == 0)
            {
                throw new IPv4Exception("Unable to obtain local IP addresses!");
            }
            foreach (IPAddress ip in myIps)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return new IPEndPoint(ip, endPointPort);
                }
            }
            throw new IPv4Exception("Unable to create IPV4 endpoint - no IPV4 addresses available!");
        }

        public static IPAddress getIPv4Address()
        {
            if (String.IsNullOrEmpty(Dns.GetHostName()))
            {
                throw new IPv4Exception("Unable to obtain local host name!");
            }
            IPAddress[] myIps = Dns.GetHostAddresses(Dns.GetHostName());

            if (myIps == null || myIps.Length == 0)
            {
                throw new IPv4Exception("Unable to obtain local IP addresses!");
            }
            foreach (IPAddress ip in myIps)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    // not working anyways so probably just forget this attempt to make IP resolvable from the gateway
                    //if (ip.ToString().StartsWith("192.168.")) // not resolvable externally! try and grab another IP
                    //{
                    //    continue;
                    //}
                    return ip;
                }
            }
            throw new IPv4Exception("Unable to obtain IPV4 address - no IPV4 addresses available!");
        }

        public static string getIPv4HostName()
        {
            return Dns.GetHostName();
        }
    }

    public class IPv4Exception : ApplicationException
    {
        public IPv4Exception() : base() { }

        public IPv4Exception(string message) : base(message, new Exception(message)) { }
    }
}
