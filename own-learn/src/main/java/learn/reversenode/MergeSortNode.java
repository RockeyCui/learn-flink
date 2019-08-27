package learn.reversenode;

/**
 * @author cuishilei
 * @date 2019/8/8
 */
public class MergeSortNode {
    public static void main(String[] args) {
        Node node1 = new Node(1);
        Node node2 = new Node(3);
        Node node3 = new Node(5);
        Node node4 = new Node(7);
        node1.next = node2;
        node2.next = node3;
        node3.next = node4;

        Node node5 = new Node(2);
        Node node6 = new Node(4);
        Node node7 = new Node(6);
        Node node8 = new Node(8);
        node5.next = node6;
        node6.next = node7;
        node7.next = node8;

        Node fun = fun(node1, node5);

        System.out.println(fun);
    }

    private static Node fun(Node node1, Node node2) {
        if (node1 == null) {
            return node2;
        }
        if (node2 == null) {
            return node1;
        }
        Node newHead;
        if (node1.value < node2.value) {
            newHead = node1;
            node1 = node1.next;
            newHead.next = fun(node1, node2);
        } else {
            newHead = node2;
            node2 = node2.next;
            newHead.next = fun(node1, node2);
        }
        return newHead;
    }
}
