import java.io.File;
import java.io.FileInputStream;
import java.io.FilePermission;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DBConfig {

	public static int pagesize = 1024;

	public static int numofPages = 0;

	public static String pathforData = ".";

	public static String tablefileextcsv = ".csv";

	public static String tablefileextdata = ".data";
	
	public static String pathforConfig = "./config.txt";
	
	public static List<String> listtablename = new ArrayList<String>();

	public static final String underscore = "_";

	public static void loadConfig(String filename) {
		int page_size = 0, totalpage = 0;
		List<String> table_names = new ArrayList<String>();
		StringBuffer line = new StringBuffer();
		String pathData = null;
	//	System.out.println("file path="+filename);
		try {
			FileInputStream fis = new FileInputStream(filename);
		/*	RandomAccessFile raf = new RandomAccessFile(new File(filename), "r");
			String line1 = raf.readLine();
			String line2 = raf.ra
		*/
			int r;
			char c;

			int flag = 1;
			while ((r = fis.read()) != -1) {
				c = (char) r;
				// System.out.print(c);
				// System.out.print("--33---");
				//System.out.println("--1--");
				while (c != '\n') {
					//System.out.println("c="+c);
					line.append(c);
					r = fis.read();
					c = (char) r;

				}
		//		System.out.println("--2--");
				if (flag == 1) {
				//	System.out.println("Line 1-->"+line+"---");
					
					String str[] = line.toString().split(" ");

					// //System.out.println(str[1]);
					// System.out.print("--1--");
					page_size = Integer.parseInt(str[1]);
					flag = 2;
					line.setLength(0);
				}

				else if (flag == 2) {
			//		System.out.print("Line 2-->"+line+"---");
					String str[] = line.toString().split(" ");
					// //System.out.println(str[0]);
					// //System.out.println("--2--");
					totalpage = Integer.parseInt(str[1]);
					flag = 3;
					line.setLength(0);
				}

				else if (flag == 3) {

					String str[] = line.toString().split(" ");
					// System.out.print(str[0]);
					pathData = str[1];
					flag = 4;
					line.setLength(0);
				}

				else if (flag == 4) {
					// //System.out.println(line);
					if (line.toString().equalsIgnoreCase("begin")) {

						line.setLength(0);
						r = fis.read();
						c = (char) r;
						while (c != '\n') {

							line.append(c);
							r = fis.read();
							c = (char) r;

						}
						// //System.out.println(line);
						// //System.out.println("--1--");
						table_names.add(line.toString());
						line.setLength(0);

					} else
						line.setLength(0);

				}
				// //System.out.println("Reading next line---");

			}

		//	//System.out.println(page_size);
		//	//System.out.println(totalpage);
		//	//System.out.println(pathData);
		//	//System.out.println(table_names);
			DBConfig.pagesize = page_size;
			DBConfig.numofPages = totalpage;
			DBConfig.pathforData = pathData;
			DBConfig.listtablename = table_names;

			fis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	

}
