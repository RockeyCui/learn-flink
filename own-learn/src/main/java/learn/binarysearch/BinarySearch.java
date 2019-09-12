package learn.binarysearch;

/**
 * @author RockeyCui
 */
public class BinarySearch {

    /**
     * 二分查找非递归实现
     *
     * @param list  有序数组
     * @param value 查找的值
     * @return java.lang.Integer
     * @author cuishilei
     * @date 2019/7/9
     */
    private static Integer binarySearch(int[] list, int value) {
        int low = 0;
        int high = list.length - 1;

        //如果前后索引没有“相遇”，则继续二分
        while (low <= high) {
            //得到“中间值”的索引
            int mid = (low + high) / 2;
            //中间值
            int hitValue = list[mid];
            //直接命中
            if (hitValue == value) {
                return mid;
            }
            //如果中间值大于 value ，则 value 在前半段
            if (hitValue > value) {
                high = mid - 1;
            } else {//否则 value 在后半段
                low = mid + 1;
            }
        }
        return null;
    }

    /**
     * 二分查找递归实现
     *
     * @param list  有序数组
     * @param low   分区前
     * @param high
     * @param value
     * @return java.lang.Integer
     * @author cuishilei
     * @date 2019/7/9
     */
    private static Integer binarySearch(int[] list, int low, int high, int value) {
        if (low <= high) {
            //得到“中间值”的索引
            int mid = (low + high) / 2;
            //中间值
            int hitValue = list[mid];
            //直接命中
            if (hitValue == value) {
                return mid;
            }
            //如果中间值大于 value ，则 value 在前半段
            if (hitValue > value) {
                return binarySearch(list, low, mid - 1, value);
            } else {//否则 value 在后半段
                return binarySearch(list, mid + 1, high, value);
            }
        }
        return null;
    }

    public static void main(String[] args) {
        int[] list = {1, 4, 6, 9, 22};

        System.out.println(binarySearch(list, 6));
        System.out.println(binarySearch(list, 0, list.length - 1, 6));
    }
}
