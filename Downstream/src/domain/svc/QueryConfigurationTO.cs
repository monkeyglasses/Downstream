using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class QueryConfigurationTO
    {
        public bool packed = true;
        public string file;
        public string fullyQualifiedFile;
        public string fields;
        public string xref;
        public string identifier;
        public string identifiedFiles;
        public string from;
        public string part;
        public string screen;
        public string wpOrComputedFields;
        public string getsAlignment;
        public bool hasChildren;

        public QueryConfigurationTO(QueryConfiguration queryConfig)
        {
            this.fields = queryConfig.Fields;
            this.file = queryConfig.File;
            this.from = queryConfig.From;
            this.fullyQualifiedFile = queryConfig.FullyQualifiedFile;
            this.getsAlignment = queryConfig.Gets_Alignment;
            this.hasChildren = queryConfig.HasChildren;
            this.identifiedFiles = queryConfig.IdentifiedFiles;
            this.identifier = queryConfig.Identifier;
            this.packed = queryConfig.Packed;
            this.part = queryConfig.Part;
            this.screen = queryConfig.Screen;
            this.wpOrComputedFields = queryConfig.WP_OR_COMPUTED_FIELDS;
            this.xref = queryConfig.XREF;
        }

        public QueryConfiguration convertToQueryConfiguration()
        {
            return new QueryConfiguration()
            {
                Fields = this.fields,
                File = this.file,
                From = this.from,
                FullyQualifiedFile = this.fullyQualifiedFile,
                Gets_Alignment = this.getsAlignment,
                HasChildren = this.hasChildren,
                IdentifiedFiles = this.identifiedFiles,
                Identifier = this.identifier,
                Packed = this.packed,
                Part = this.part,
                Screen = this.screen,
                WP_OR_COMPUTED_FIELDS = this.wpOrComputedFields,
                XREF = this.xref
            };
        }
    }
}
