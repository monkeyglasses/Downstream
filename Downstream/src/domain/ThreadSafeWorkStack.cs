using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain
{
    public class ThreadSafeWorkStack
    {
        static readonly object _locker = new object();
        private Stack<ExtractorConfiguration> _workStack = new Stack<ExtractorConfiguration>();

        public void SortBySiteCode()
        {
            lock (_locker)
            {
                if (_workStack == null || _workStack.Count == 0)
                {
                    return;
                }

                Dictionary<String, IList<ExtractorConfiguration>> dictForSorting = new Dictionary<string, IList<ExtractorConfiguration>>();

                while (_workStack.Count > 0)
                {
                    ExtractorConfiguration current = _workStack.Pop();

                    if (!dictForSorting.ContainsKey(current.SiteCode))
                    {
                        dictForSorting.Add(current.SiteCode, new List<ExtractorConfiguration>());
                    }
                    dictForSorting[current.SiteCode].Add(current);
                }

                // push jobs back on stack by sitecode
                foreach (String key in dictForSorting.Keys)
                {
                    foreach (ExtractorConfiguration ec in dictForSorting[key])
                    {
                        _workStack.Push(ec);
                    }
                }
            }
        }

        public IList<ExtractorConfiguration> CopyTo(IList<ExtractorConfiguration> destination)
        {
            if (destination == null)
            {
                destination = new List<ExtractorConfiguration>();
            }
            lock (_locker)
            {
                foreach (ExtractorConfiguration config in _workStack)
                {
                    ExtractorConfiguration newConfig = config.Clone();
                    destination.Add(newConfig);
                }
            }
            return destination;
        }

        /// <summary>
        /// Look for a configuration that matches the site code and vista file
        /// </summary>
        /// <param name="config"></param>
        /// <returns>Returns the ExtractorConfiguration object if found - null otherwise</returns>
        public ExtractorConfiguration Find(string sitecode, string vistaFile)
        {
            if (String.IsNullOrEmpty(sitecode) || String.IsNullOrEmpty(vistaFile))
            {
                return null;
            }
            lock (_locker)
            {
                foreach (ExtractorConfiguration ec in _workStack)
                {
                    if (ec.QueryConfigurations.RootNode.Value.File == vistaFile && ec.SiteCode == sitecode)
                    {
                        return ec;
                    }
                }
            }
            return null;
        }

        public void Remove(ExtractorConfiguration config)
        {
            if (config == null || _workStack == null || _workStack.Count == 0)
            {
                return;
            }
            lock (_locker)
            {
                Stack<ExtractorConfiguration> temp = new Stack<ExtractorConfiguration>();
                while (_workStack.Count != 0)
                {
                    temp.Push(_workStack.Pop());
                    ExtractorConfiguration peeker = temp.Peek();
                    if (String.Equals(peeker.QueryConfigurations.RootNode.Value.File, config.QueryConfigurations.RootNode.Value.File) && 
                        String.Equals(peeker.SiteCode, config.SiteCode))
                    {
                        temp.Pop(); // if this last one matched the config argument then pop it off to nowhere
                    }
                }
                _workStack = temp;
            }
        }

        public void RemoveAll()
        {
            lock (_locker)
            {
                _workStack = new Stack<ExtractorConfiguration>();
            }
        }

        public ExtractorConfiguration Pop()
        {
            lock (_locker)
            {
                if (_workStack.Count > 0)
                {
                    return _workStack.Pop();
                }
                else
                {
                    return null;
                }
            }
        }

        public ExtractorConfiguration PopSiteUnique(ThreadSafeWorkStack activeJobs)
        {
            ExtractorConfiguration retVal = null;
            lock (_locker)
            {
                if (_workStack.Count > 0)
                {
                    ExtractorConfiguration lcv = null;
                    Stack<ExtractorConfiguration> bucket = new Stack<ExtractorConfiguration>();
                    while ((lcv = _workStack.Pop()) != null)
                    {
                        if (activeJobs.locklessFind(lcv.SiteCode))
                        {
                            bucket.Push(lcv);
                            if (_workStack.Count > 0) continue; else break;
                        }
                        else
                        {
                            retVal = lcv;
                            break;
                        }
                    }
                    if (bucket.Count > 0)
                    {
                        while ((lcv = bucket.Pop()) != null)
                        {
                            _workStack.Push(lcv);
                            if (bucket.Count > 0) continue; else break;
                        }
                    }
                }
            }
            return retVal;
        }

        private bool locklessFind(String sitecode)
        {
            bool exists = false;
            foreach (ExtractorConfiguration config in _workStack.ToArray())
            {
                if (config.SiteCode.Equals(sitecode))
                {
                    exists = true;
                    break;
                }
            }
            return exists;
        }

        public bool ExistsByFile(String file)
        {
            bool exists = false;
            lock (_locker)
            {
                foreach (ExtractorConfiguration config in _workStack.ToArray())
                {
                    if (config.QueryConfigurations.RootNode.Value.File.Equals(file))
                    {
                        exists = true;
                        break;
                    }
                }
            }
            return exists;
        }

        public void Push(ExtractorConfiguration config)
        {
            lock (_locker)
            {
                _workStack.Push(config);
            }
        }

        public void PushOrUpdate(ExtractorConfiguration config)
        {
            Remove(config);
            lock (_locker)
            {
                _workStack.Push(config);
            }
        }

        public Int32 Count()
        {
            lock (_locker)
            {
                return _workStack.Count;
            }
        }

        public bool Contains(ExtractorConfiguration config)
        {
            if (config == null || String.IsNullOrEmpty(config.QueryConfigurations.RootNode.Value.File) || String.IsNullOrEmpty(config.SiteCode))
            {
                return false;
            }
            bool found = false;
            lock (_locker)
            {
                foreach (ExtractorConfiguration ec in _workStack)
                {
                    if (ec.QueryConfigurations.RootNode.Value.File.Equals(config.QueryConfigurations.RootNode.Value.File) && ec.SiteCode.Equals(config.SiteCode))
                    {
                        found = true;
                        break;
                    }
                }
            }
            return found;
        }

        public void Prioritize(IList<ExtractorConfiguration> prioritizedExtractions)
        {
            lock (_locker)
            {
                foreach (ExtractorConfiguration config in prioritizedExtractions)
                {
                    Remove(config);
                }
                foreach (ExtractorConfiguration config in prioritizedExtractions)
                {
                    _workStack.Push(config); // just push these right on top of the stack
                }
            }
        }

        /// <summary>
        /// Search the stack (without locking) for a site
        /// </summary>
        /// <param name="sitecode"></param>
        /// <returns>true if any configurations exist for the sitecode</returns>
        public bool ContainsBySite(String sitecode)
        {
            if (_workStack == null || _workStack.Count == 0)
            {
                return false;
            }

            foreach (ExtractorConfiguration config in _workStack)
            {
                if (String.Equals(config.SiteCode, sitecode))
                {
                    return true;
                }
            }
            return false;
        }
    }
}
