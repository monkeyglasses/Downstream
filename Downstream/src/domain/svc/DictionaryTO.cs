using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class TaggedExtractorConfigArray : AbstractArrayTO
    {
        public String key;
        public ExtractorConfigurationTO[] value;

        public TaggedExtractorConfigArray() { }

        public TaggedExtractorConfigArray(String key, IList<ExtractorConfiguration> values)
        {
            this.key = key;
            this.value = new ExtractorConfigurationTO[values.Count];
            this.count = values.Count;
            for (int i = 0; i < values.Count; i++)
            {
                this.value[i] = new ExtractorConfigurationTO(values[i]);
            }
        }

        public IList<ExtractorConfiguration> convertToExtractorConfigurationList()
        {
            IList<ExtractorConfiguration> result = new List<ExtractorConfiguration>();

            if (this.value == null || this.value.Length == 0)
            {
                return result;
            }

            for (int i = 0; i < this.value.Length; i++)
            {
                result.Add(value[i].convertToExtractorConfiguration());
            }

            return result;
        }
    }

    [Serializable]
    public class TaggedExtractorConfigArrays : AbstractArrayTO
    {
        public TaggedExtractorConfigArray[] values;

        public TaggedExtractorConfigArrays() { }

        public TaggedExtractorConfigArrays(Dictionary<String, IList<ExtractorConfiguration>> dict)
        {
            if (dict == null || dict.Count == 0)
            {
                return;
            }
            values = new TaggedExtractorConfigArray[dict.Count];
            this.count = dict.Count;
            int index = 0;
            foreach (String key in dict.Keys)
            {
                values[index] = new TaggedExtractorConfigArray(key, dict[key]);
                index++;
            }
        }

        public Dictionary<String, IList<ExtractorConfiguration>> convertToDictionary()
        {
            Dictionary<string, IList<ExtractorConfiguration>> result = new Dictionary<string,IList<ExtractorConfiguration>>();

            if (values == null || values.Length == 0)
            {
                return result;
            }

            for (int i = 0; i < this.values.Length; i++)
            {
                if (!result.ContainsKey(this.values[i].key))
                {
                    result.Add(this.values[i].key, new List<ExtractorConfiguration>());
                }
            }

            if (this.values != null && this.values.Length > 0)
            {
                for (int i = 0; i < this.values.Length; i++)
                {
                    result[this.values[i].key] = this.values[i].convertToExtractorConfigurationList();
                }
            }

            return result;
        }
    }

    #region Lite Messaging

    [Serializable]
    public class TaggedExtractorConfigArrayLite : AbstractArrayTO
    {
        public String key;
        public ExtractorConfigurationTOLite[] value;

        public TaggedExtractorConfigArrayLite() { }

        public TaggedExtractorConfigArrayLite(String key, IList<ExtractorConfiguration> values)
        {
            this.key = key;
            this.value = new ExtractorConfigurationTOLite[values.Count];
            this.count = values.Count;
            for (int i = 0; i < values.Count; i++)
            {
                this.value[i] = new ExtractorConfigurationTOLite(values[i]);
            }
        }

        public TaggedExtractorConfigArrayLite(TaggedExtractorConfigArray teca)
        {
            this.key = teca.key;
            this.value = new ExtractorConfigurationTOLite[teca.count];
            this.count = teca.count;
            for (int i = 0; i < teca.count; i++)
            {
                this.value[i] = new ExtractorConfigurationTOLite(teca.value[i]);
            }
        }
    }

    [Serializable]
    public class TaggedExtractorConfigArraysLite : AbstractArrayTO
    {
        public TaggedExtractorConfigArrayLite[] values;

        public TaggedExtractorConfigArraysLite() { }

        public TaggedExtractorConfigArraysLite(Dictionary<String, IList<ExtractorConfiguration>> dict)
        {
            if (dict == null || dict.Count == 0)
            {
                return;
            }
            this.count = dict.Count;
            values = new TaggedExtractorConfigArrayLite[dict.Count];
            int index = 0;
            foreach (String key in dict.Keys)
            {
                values[index] = new TaggedExtractorConfigArrayLite(key, dict[key]);
                index++;
            }
        }

        public TaggedExtractorConfigArraysLite(TaggedExtractorConfigArrays teca)
        {
            if (teca == null || teca.count == 0)
            {
                return;
            }
            this.count = teca.count;
            values = new TaggedExtractorConfigArrayLite[teca.count];
            for (int i = 0; i < teca.values.Length; i++)
            {
                values[i] = new TaggedExtractorConfigArrayLite(teca.values[i]);
            }
        }

    }

    #endregion


}
