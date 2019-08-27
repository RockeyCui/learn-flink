package learn.reversenode;

/**
 * @author cuishilei
 * @date 2019/8/8
 */
public class ReverseNode {

    public static void main(String[] args) {
        Node node1 = new Node(1);
        Node node2 = new Node(2);
        Node node3 = new Node(3);
        Node node4 = new Node(4);
        node1.next = node2;
        node2.next = node3;
        node3.next = node4;

        Node fun = loopReverse(node1);

        System.out.println(fun);
    }

    /**
     * 遍历法反转单向链表
     *
     * @param head 链表头
     * @return 新的链表头
     * @author cuishilei
     * @date 2019/8/8
     */
    private static Node loopReverse(Node head) {
        //保存先前节点信息
        Node pre = null;
        //临时转换变量
        Node next;
        // 1->2->3->4->5
        while (head != null) {
            //head=1 next = 2 |head=2 next = 3  | head=3 next=4
            next = head.next;
            //1->null         | 2->1->null      | 3->2->1->null
            head.next = pre;
            //pre=1           | pre=2           | pre=3
            pre = head;
            //head = 2        | head=3          | head=4
            head = next;
        }
        return pre;
    }

    /**
     * 递归反转单向链表
     *
     * @param head 链表头
     * @return 新的链表头
     * @author cuishilei
     * @date 2019/8/8
     */
    public static Node reverse(Node head) {
        //如果 head 为 null，或者下个节点为 null 直接返回
        if (head == null || head.next == null) {
            return head;
        }
        Node temp = head.next;
        Node newHead = reverse(head.next);
        temp.next = head;
        head.next = null;
        return newHead;
    }
}
