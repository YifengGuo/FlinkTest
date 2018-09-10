package java_properties;

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by guoyifeng on 9/10/18
 */
public class JavaEnvTest {
    @Test
    public void testJavaEnv() throws Exception {
        FileWriter fw = new FileWriter(new File("src/main/java/java_properties/EnvironmentVariables.txt"));
        Map<String, String> envMap = System.getenv();
        envMap.forEach((k, v) -> {
            System.out.println(k + " ========> " + v);
            try {
                fw.write(k + " ========> " + v + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        fw.flush();
        fw.close();
    }
}
