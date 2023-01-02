package edu.upenn.cis.db.postgres;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PostgresTest {
	private String dbname = "test42324";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		// Postgres.disconnect();
	}

	@Test
	public void testDropDatabase() {
		// Postgres.createDatabase(dbname);
		// Postgres.dropDatabase(dbname);
	}

	@Test
	public void testUseDatabase() {
		// Postgres.createDatabase(dbname);
		// Postgres.useDatabase(dbname);
		// Postgres.dropDatabase(dbname);
	}

}
