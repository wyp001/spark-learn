import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


/**
 * 生成目录列表
 */
public class T1 {
    public static void main(String[] args) throws Exception {
        String titlePath = "D:\\Test\\title.txt";
        String hrefPath = "D:\\Test\\href.txt";
        String readmeFile =  "D:\\Test\\README.md";

        String baseUrl = "https://www.bilibili.com";

        List<String> titles = readFile(titlePath);
        List<String> hrefs = readFile(hrefPath);

        FileWriter writer = new FileWriter(readmeFile);

        for (int i = 0; i < titles.size(); i++) {
            String title = titles.get(i).replace("尚硅谷_", "");
            String url = baseUrl + hrefs.get(i);
            String resStr = "#### [" + title + "](" + url + ")\n";
            writer.write(resStr);
        }

    }

    public static List<String> readFile(String path) throws IOException {
        // 使用一个字符串集合来存储文本中的路径 ，也可用String []数组
        List<String> list = new ArrayList<String>();
        FileInputStream fis = new FileInputStream(path);
        // 防止路径乱码   如果utf-8 乱码  改GBK     eclipse里创建的txt  用UTF-8，在电脑上自己创建的txt  用GBK
        InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);
        String line = "";
        while ((line = br.readLine()) != null) {
            // 如果 t x t文件里的路径 不包含---字符串       这里是对里面的内容进行一个筛选
            if (line.lastIndexOf("---") < 0) {
                list.add(line);
            }
        }
        br.close();
        isr.close();
        fis.close();
        return list;
    }

}
