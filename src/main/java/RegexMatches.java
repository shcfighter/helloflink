import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexMatches {
	
	public static void main(String args[]) {
		String str = "[2019-10-11 17:11:55:687] [INFO] [vert.x-eventloop-thread-1] [com.ecit.shop.api.RestCaloriesRxVerticle.searchHandler(RestCaloriesRxVerticle.java:136)] - 122.224.90.178 查询食物：苹果";
		String pattern = "\\[([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3})\\]\\s\\[([A-Z]*)\\]\\s\\[([\\w\\.-]*)\\]\\s\\[([\\w\\.():]*)\\]\\s-\\s([\\s\\S]*)";

		/*String str = "122.224.90.178 查询食物苹果";
		String pattern = "[\\s\\S]*";*/
		Pattern r = Pattern.compile(pattern);
		Matcher matcher = r.matcher(str);
		System.out.println(matcher.matches());
		System.out.println(matcher.group(1));
		System.out.println(matcher.group(2));
		System.out.println(matcher.group(3));
		System.out.println(matcher.group(4));
		System.out.println(matcher.group(5));
	}

}