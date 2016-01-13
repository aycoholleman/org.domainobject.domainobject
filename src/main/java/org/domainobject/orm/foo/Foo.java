package org.domainobject.orm.foo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.domainobject.orm.core.Context;
import org.domainobject.orm.core.MetaData;
import org.domainobject.orm.persister.StandardPersister;
import org.domainobject.util.debug.BeanPrinter;

public class Foo {

	public static void main(String[] args) throws Exception
	{

		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/domainobject", "root", null);

		//		DatabaseMetaData dbmd = conn.getMetaData();
		//		ResultSet rs = dbmd.getPrimaryKeys("pro3_piping_tools", null,"xmap");
		//		while (rs.next()) {
		//			System.out.println(rs.getString(4));
		//		}

		final long start = System.currentTimeMillis();

		Context context = new Context(conn);

		StandardPersister persister = new StandardPersister(context);

		MetaData<ArticleCategory> acMetaData = Context.getDefaultContext().createMetaData(ArticleCategory.class);
		ArticleCategory category = persister.load(new ArticleCategory(), 4);
		new BeanPrinter().dump(category);

		Article article = persister.load(new Article(), 2, "name");

		List<Article> articles = persister.loadChildren(category, Article.class, "catId").asList();

		ArticleCategory ac = persister.loadParent(articles.get(0), new ArticleCategory(), new String[] { "name" }, "catId");

		article = new Article();
		article.name = "Hamer 6";
		article.description = "Houten hamer 3";

		persister.save(article);
		new BeanPrinter().dump(article);

		article.description = "Ijzeren hamer";
		persister.update(article);

		System.out.println("Article count: " + persister.count(Article.class));

		Supplier supplier = new Supplier();
		supplier.setName("Ayco");
		supplier.setTelephone1("1234567");
		supplier.setEmail1("test@test.com");
		supplier.setUrl1("www.test.abc.com");
		for (int i = 0; i < 5; ++i)
			supplier.save();

		//new BeanPrinter().dump(supplier);

		System.out.println("seconds: " + (((double) System.currentTimeMillis() - (double) start) / (double) 1000));
	}
}
