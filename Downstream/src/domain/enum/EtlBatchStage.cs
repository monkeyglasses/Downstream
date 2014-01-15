using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace com.bitscopic.downstream.domain
{
	public enum EtlBatchStage
	{
        NOT_STARTED = 0,

        START_WORKFLOW = 1156870,
        COMPLETED_WORKFLOW = 1156885,

        PHARMA_STAGING = 1156874,
        PHARMA_NDS = 1156875,
        PHARMA_DATA_MART = 1156876,

        LAB_MICRO_STAGING = 1156877,
        LAB_MICRO_NDS = 1156878,
        LAB_MICRO_DATA_MART = 1156879,

        BCMA_STAGING = 1156880,
        BCMA_NDS = 1156881,
        BCMA_DATA_MART = 1156882,

        VITALS_STAGING = 1156883,
        VITALS_NDS = 1156884,
        //VITALS_DATA_MART,

        PATIENT_ADT_STAGING = 1156871,
        PATIENT_ADT_NDS = 1156872,
        PATIENT_ADT_DATA_MART = 1156873
	}
}