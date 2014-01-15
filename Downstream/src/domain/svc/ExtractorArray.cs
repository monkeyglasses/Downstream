using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class ExtractorArray : AbstractArrayTO
    {
        public ExtractorTO[] extractors;

        public ExtractorArray() { }

        public ExtractorArray(IList<Extractor> extractors)
        {
            if (extractors == null || extractors.Count == 0)
            {
                return;
            }

            this.count = extractors.Count;
            this.extractors = new ExtractorTO[extractors.Count];
            for (int i = 0; i < extractors.Count; i++)
            {
                this.extractors[i] = new ExtractorTO(extractors[i]);
            }
        }
    }

    #region Lite Messaging

    [Serializable]
    public class ExtractorArrayLite : AbstractArrayTO
    {
        public ExtractorTOLite[] extractors;

        public ExtractorArrayLite() { }

        public ExtractorArrayLite(IList<Extractor> extractors)
        {
            if (extractors == null || extractors.Count == 0)
            {
                return;
            }

            this.count = extractors.Count;
            this.extractors = new ExtractorTOLite[extractors.Count];
            for (int i = 0; i < extractors.Count; i++)
            {
                this.extractors[i] = new ExtractorTOLite(extractors[i]);
            }
        }
    }

    #endregion

}
