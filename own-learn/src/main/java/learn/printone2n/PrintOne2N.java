package learn.printone2n;

/**
 * @author cuishilei
 * @date 2019/8/8
 */
public class PrintOne2N {
    public static void main(String[] args) {
        fun(2);
    }

    /**
     * 输入 N 打印 1 至 N 位最大数
     *
     * @param n 位数
     * @author cuishilei
     * @date 2019/8/8
     */
    private static void fun(int n) {
        String[] num = new String[n];
        for (int i = 0; i < n; i++) {
            num[i] = "0";
        }
        while (!increment(num)) {
            printNumber(num);
        }
    }

    /**
     * 加 1 操作
     *
     * @param number 代表数字的数组
     * @return boolean 是否到达最大值
     * @author cuishilei
     * @date 2019/8/8
     */
    private static boolean increment(String[] number) {
        //判断是否是最大数
        boolean isMax = false;
        //进位
        int carry = 0;
        int length = number.length;
        for (int i = length - 1; i >= 0; i--) {
            //取到第 i 位的数字并加上进位符
            int nSum = Integer.parseInt(number[i]) + carry;
            //加 1 操作
            if (i == length - 1) {
                nSum++;
            }
            //如果加 1 后此位超过 10，则需进位
            if (nSum >= 10) {
                //如果发现是数字第一位超过 10 了，则此时已经是 N 位数最大了
                if (i == 0) {
                    //告诉外层已经最大数了，不要加了，跳出循环
                    isMax = true;
                    break;
                } else {
                    //不是第一位数，继续计算
                    nSum -= 10;
                    carry = 1;
                    number[i] = String.valueOf(nSum);
                }
            } else {
                //加1后此位没有超过 10，也不用进位，所以跳出循环
                number[i] = String.valueOf(nSum);
                break;
            }
        }
        return isMax;
    }

    /**
     * 打印操作
     *
     * @param number 代表数字的数组
     * @author cuishilei
     * @date 2019/8/8
     */
    private static void printNumber(String[] number) {
        boolean isZero = true;
        int length = number.length;
        for (String s : number) {
            if (isZero && !"0".equals(s)) {
                isZero = false;
            }
            if (!isZero) {
                System.out.print(s);
            }
        }
        System.out.println();
    }
}
