package java_properties;

/**
 * Created by guoyifeng on 9/10/18
 */
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Properties;
import java.util.Set;
public class JavaPropertiesTest {
    @Test
    public void testPropertiesList() throws Exception {
        FileWriter fw = new FileWriter(new File("src/main/java/java_properties/JavaPropertiesList.txt"));
        Properties p = System.getProperties();
        Set<String> names = p.stringPropertyNames();
        for (String name : names) {
            System.out.println(name + " =========> " + p.getProperty(name));
            fw.write(name + " =========> " + p.getProperty(name) + "\n");
        }
        if (fw != null) {
            fw.flush();
            fw.close();
        }
    }
}
