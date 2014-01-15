using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain
{
    public class ThreadSafeExtractorList
    {
        static readonly object _locker = new object();
        private Dictionary<string, Extractor> _extractorDictionary = new Dictionary<string, Extractor>();

        /// <summary>
        /// Get the number of extractors
        /// </summary>
        /// <returns></returns>
        public Int32 Count()
        {
            lock (_locker)
            {
                return _extractorDictionary.Count;
            }
        }

        /// <summary>
        /// Gets the list of Extractor objects
        /// </summary>
        public IList<Extractor> GetExtractors()
        {
            if (_extractorDictionary == null || _extractorDictionary.Count == 0)
            {
                return new List<Extractor>();
            }
            else
            {
                IList<Extractor> result = new List<Extractor>();
                lock (_locker)
                {
                    foreach (Extractor e in _extractorDictionary.Values)
                    {
                        Extractor newExtractor = new Extractor(e.HostName, e.ListeningPort, e.SiteCode, e.VistaFile, e.Timestamp);
                        result.Add(newExtractor);
                    }
                }
                return result;
            }
        }

        /// <summary>
        /// Add a new Extractor object to the collection
        /// </summary>
        /// <param name="extractor"></param>
        public void Add(Extractor extractor)
        {
            if (extractor == null || String.IsNullOrEmpty(extractor.SiteCode) || String.IsNullOrEmpty(extractor.VistaFile))
            {
                throw new ArgumentException("The supplied extractor object is incomplete!");
            }
            string compositeKey = extractor.SiteCode + "_" + extractor.VistaFile;
            lock (_locker)
            {
                if (_extractorDictionary.ContainsKey(compositeKey))
                {
                    return;
                }
                else
                {
                    _extractorDictionary.Add(compositeKey, extractor);
                }
            }
        }

        /// <summary>
        /// Remove an Extractor object from the collection
        /// </summary>
        /// <param name="extractor"></param>
        public void Remove(Extractor extractor)
        {
            if (extractor == null || String.IsNullOrEmpty(extractor.SiteCode) || String.IsNullOrEmpty(extractor.VistaFile))
            {
                throw new ArgumentException("The supplied extractor object is incomplete!");
            }
            string compositeKey = extractor.SiteCode + "_" + extractor.VistaFile;
            lock (_locker)
            {
                if (!_extractorDictionary.ContainsKey(compositeKey))
                {
                    return;
                }
                else
                {
                    _extractorDictionary.Remove(compositeKey);
                }
            }
        }

        /// <summary>
        /// Check the collection of Extractor objects for an instance of the specified Extractor object
        /// </summary>
        /// <param name="extractor"></param>
        /// <returns></returns>
        public bool Contains(Extractor extractor)
        {
            if (extractor == null || String.IsNullOrEmpty(extractor.VistaFile) || String.IsNullOrEmpty(extractor.SiteCode))
            {
                return false;
            }
            string compositeKey = extractor.SiteCode + "_" + extractor.VistaFile;
            lock (_locker)
            {
                return _extractorDictionary.ContainsKey(compositeKey);
            }
        }
    }
}
