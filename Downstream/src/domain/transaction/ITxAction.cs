using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.transaction
{
    interface ITxAction
    {
        object execute();
        object rollback();
    }
}
