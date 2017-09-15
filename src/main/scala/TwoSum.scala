
class ListNode(var _x: Int = 0) {
  var next: ListNode = null
  var x: Int = _x
}

object TwoSum {
  def twoSum(nums: Array[Int], target: Int): Array[Int] = {
    val withIndex = nums.zipWithIndex
    withIndex.foreach(x => {
      withIndex.foreach(y => {
        if (x._2 != y._2 && x._1 + y._1 == target) return Array(x._2, y._2)
      })
    })
    Array()
  }


  def addTwoNumbers(l1: ListNode, l2: ListNode): ListNode = {
    var carry = 0
    val newHead = new ListNode(0)
    var p1 = l1
    var p2 = l2
    var p3 = newHead
    while ( {
      p1 != null || p2 != null
    }) {
      if (p1 != null) {
        carry += p1.x
        p1 = p1.next
      }
      if (p2 != null) {
        carry += p2.x
        p2 = p2.next
      }
      p3.next = new ListNode(carry % 10)
      p3 = p3.next
      carry /= 10
    }
    if (carry == 1) p3.next = new ListNode(1)
    newHead.next
  }


  def main(args: Array[String]): Unit = {


  }
}