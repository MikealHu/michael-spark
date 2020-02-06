package cn.michael.spark.util

/**
 * Created by hufenggang on 2020/1/10.
 */

/** CallSite represents a place in user code. It can have a short and a long form. */
private[spark] case class CallSite(shortForm: String, longForm: String)

private[spark] object CallSite {
    val SHORT_FORM = "callSite.short"
    val LONG_FORM = "callSite.long"
    val empty = CallSite("", "")
}
