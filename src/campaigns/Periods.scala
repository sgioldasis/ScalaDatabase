/*
 * Periods.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package campaigns

object Periods {

    val timeKeyFrom = m.campaign.obj.timeKeyFrom
    val sysTimeKey  = m.systime.obj.sysTimeKey
    val bocTimeKey  = m.campaign.obj.timeKeyBoc
    val indPeriod   = m.systime.obj.sysTimeKey.substring(0, 1)

    val periodMonth = indPeriod == "M"
    val periodWeek  = indPeriod == "W"
    val periodDay   = indPeriod == "D"

    val wCurYear    = m.systime.obj.sysTimeKey.substring(1, 5)
    val wCurWeek    = m.systime.obj.sysTimeKey.substring(5, 7)
    val wCurWeekNo  = wCurWeek.toInt
    val wPrevWeekNo = wCurWeekNo - 1
    val wPrevWeek   = if (wPrevWeekNo > 9) wPrevWeekNo+"" else "0"+wPrevWeekNo

    val mCurYear    = m.systime.obj.sysTimeKey.substring(1, 5)
    val mCurMonth   = m.systime.obj.sysTimeKey.substring(5, 7)
    val mCurYearNo  = mCurYear.toInt
    val mPrevYearNo = mCurYear.toInt - 1
    val mPrevYear   = ""+mPrevYearNo
    val mCurMonthNo = mCurMonth.toInt

    val mMnmMonthNo = if (mCurMonthNo == 12) 1            else mCurMonthNo+1
    val mMnmYearNo  = if (mCurMonthNo == 12) mCurYearNo+1 else mCurYearNo
    val mMnmMonth   = if (mMnmMonthNo > 9) ""+mMnmMonthNo else "0"+mMnmMonthNo
    val mMnmYear    = ""+mMnmYearNo

    val mMpmMonthNo = if (mCurMonthNo == 1) 12            else mCurMonthNo-1
    val mMpmYearNo  = if (mCurMonthNo == 1) mCurYearNo-1  else mCurYearNo
    val mMpmMonth   = if (mMpmMonthNo > 9) ""+mMpmMonthNo else "0"+mMpmMonthNo
    val mMpmYear    = ""+mMpmYearNo

    val mMpdMonthNo = mCurMonthNo-1
    val mMpdYearNo  = mCurYearNo
    val mMpdMonth   = if (mMpdMonthNo > 9) ""+mMpdMonthNo else "0"+mMpdMonthNo
    val mMpdYear    = ""+mMpdYearNo

    val mMbmMonthNo = if (mCurMonthNo == 1) 12            else mCurMonthNo-1
    val mMbmYearNo  = if (mCurMonthNo == 1) mCurYearNo-1  else mCurYearNo
    val mMbmMonth   = if (mMbmMonthNo > 9) ""+mMbmMonthNo else "0"+mMbmMonthNo
    val mMbmYear    = ""+mMbmYearNo

    val mRemMonths:BigDecimal =
        if (periodMonth && Periods.mCurMonth != null && Periods.mCurMonthNo <= 12)
          12 - Periods.mCurMonthNo
        else null

    def calcRelPeriods = {
        m.reltime.create( "CUR", sysTimeKey                  , sysTimeKey)
        m.reltime.create( "CTD", timeKeyFrom                 , sysTimeKey)
        m.reltime.create( "BOC", bocTimeKey                  , bocTimeKey)
        m.reltime.create( "WYD", "W" + wCurYear  + "00"      , sysTimeKey)
        m.reltime.create( "WPD", "W" + wCurYear  + wPrevWeek , sysTimeKey)
        m.reltime.create( "MNM", "M" + mMnmYear  + mMnmMonth , "M" + mMnmYear + mMnmMonth )
// IN RELTIME       m.reltime.create( "MPM", "M" + mMpmYear  + mMpmMonth , "M" + mMpmYear + mMpmMonth )
// IN RELTIME       m.reltime.create( "MBY", "M" + mCurYear  + "01"      , sysTimeKey)
//        m.reltime.create( "MBM", "M" + mCurYear  + "01"      , "M" + mMbmYear + mMbmMonth)
        m.reltime.create( "MCY", "M" + mCurYear  + "01"      , "M" + mCurYear + "12")
        m.reltime.create( "MYD", "M" + mPrevYear + "01"      , sysTimeKey)
        m.reltime.create( "MPY", "M" + mPrevYear + mCurMonth , "M" + mPrevYear + mCurMonth)
        m.reltime.create( "MPD", "M" + mMpdYear  + mMpdMonth , sysTimeKey )
        m.reltime.create( "MBD", "M" + mCurYear  + "00"      , sysTimeKey)
        m.reltime.create( "MBM", "M" + wCurYear  + "01"      , "M" + mMbmYear  + mMbmMonth)
    }

}
