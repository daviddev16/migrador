package migrador;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;

public class Migrador {

	/* mapping keys */
	public static final String OLD_VERSION_STATE 		= "postgresql:old:state"; 
	public static final String OLD_VERSION_SERVICE_NAME = "postgresql:old:serviceName";
	public static final String OLD_VERSION_HOME         = "postgresql:old:homePath";
	public static final String PG_DUMP_FILE              = "postgresql:file:pg_dump";
	public static final String PSQL_FILE                 = "postgresql:file:psql";

	/* only 9.6 stuff */
	public static final String NEW_VERSION_STATE 		= "postgresql:new:state"; 
	public static final String NEW_VERSION_SERVICE_NAME = "postgresql:new:serviceName";
	public static final String NEW_VERSION_HOME         = "postgresql:new:homePath";

	/* postgresql queries */
	public static final String QUERY_GET_ALL_DATABASES = "select datname FROM pg_database WHERE datistemplate='f' AND datacl IS NOT NULL;";

	/* all old supported versions */
	private static final List<String> OLD_VERSIONS = Arrays.asList("9.0", "9.1", "9.2", "9.3", "9.4", "9.5");
	
	private static long lastTime;
	private static long newTime;
	
	private static final Map<String, Object> ENV = Collections.synchronizedMap(new HashMap<String, Object>());

	{
		ENV.put(OLD_VERSION_STATE, VersionState.UNKNOWN);
		ENV.put(NEW_VERSION_STATE, VersionState.UNKNOWN);
		ENV.put(OLD_VERSION_SERVICE_NAME, "?");
		ENV.put(NEW_VERSION_SERVICE_NAME, "?");
		ENV.put(OLD_VERSION_HOME, null);
		ENV.put(NEW_VERSION_HOME, null);
	}

	/* version service state enum */
	public static enum VersionState { RUNNING, STOPPED, UNKNOWN; }

	public static void main(String[] args) 
			throws IOException, NullPointerException, IllegalStateException, ClassNotFoundException, SQLException {

		lastTime = System.currentTimeMillis();
		migrate("postgres","#abc123#");
		newTime = System.currentTimeMillis();
		System.out.println("Time taken: " + (newTime-lastTime)/1000 + "s");
	}

	public static void migrate(String username, String password) 
			throws NullPointerException, IllegalStateException, IOException, ClassNotFoundException, SQLException {

		/* procura por todas as versões rodando do PostgreSQL e registra no ambiente (ENV) */
		checkingServices(); 

		System.out.println(String.format("Serviços encontrados [%s, %s]", ENV.get(OLD_VERSION_SERVICE_NAME), 
				ENV.get(NEW_VERSION_SERVICE_NAME)));

		/* parar serviços para configuração de porta */
		stopServices(); 

		/* encontra o caminho para a pasta principal dos postgres*/
		findAllPostgreSQLPaths(); 

		System.out.println(String.format("Caminhos encontrados [%s, %s]", ENV.get(OLD_VERSION_HOME), 
				ENV.get(NEW_VERSION_HOME)));

		/* subir ambos os serviços */
		System.out.println("Configurando PostgreSQL para subir com a Porta 5433...");
		updatePostgresWithPort("5433", NEW_VERSION_HOME);

		System.out.println("Porta atualizada com sucesso.");
		System.out.println("Subindo serviços do PostgreSQL para iniciar a migração.");
		System.out.println("[!] Será necessário fazer a conexão com o banco de dados antigo e o novo [!]");

		/* iniciando serviços do postgresql */
		startServices();

		System.out.println("Iniciando pre-requisitos para migração...");

		final List<String> databases = queryAllDatabases("postgres", "#abc123#", "localhost", "5432", null);

		/* executando migração para versão 9.6 (porta=5433) */
		executeMigration("postgres", "#abc123#", "localhost", "5433", databases);

		debugStuff();
	}

	private static void executeMigration(String username, String password, String host, String port, List<String> databases) 
			throws ClassNotFoundException, SQLException, IOException {

		Statement statement = createStatement(username, password, host, port, "");
		createAllDatabasesBefore(statement, databases);

		System.out.println("Começando migração...");

		for (String database : databases) {
			System.out.println("Começando migração de \"" + database + "\".");
			String cmd = buildCommand(host, "5432", port, username, password, database);	
			System.out.println(cmd);

			int ec = run(cmd, false, (line) -> {
				System.out.println(line);
			}, null, new File((File)ENV.get(NEW_VERSION_HOME), "bin"));
			System.out.println("\"" + database + "\" migrada com sucesso.");
		}
	}

	private static String buildCommand(String host, String sourcePort, String destPort, String username, String password, String database) {
		return "pg_dump --host \""+host+"\" --port \""+sourcePort+"\" --verbose --username \""+username+"\" \""+database+
				"\"|psql --host \""+username+"\" --port \""+destPort+"\" --username \""+username+"\" \""+database+"\"";
	}

	private static void createAllDatabasesBefore(Statement statement, List<String> databases) throws SQLException {
		System.out.println("Criando banco de dados...");
		for (String database : databases) {
			System.out.println("Criando: \"" + database + "\".");
			boolean flag = true;
			for(String command : databaseCreationCommands(database)) {
				flag &= statement.execute(command);
			}
			if (flag == false) {
				System.out.println("Houve algum problema ao criar \"" + database + "\".");
				continue;
			}
			System.out.println("\"" + database + "\" criado.");
		}
	}


	private static List<String> queryAllDatabases(String username, String password, String host, String port, String[] except) 
			throws SQLException, ClassNotFoundException {
		Statement statement = createStatement(username, password, host, port, "");
		ResultSet resultSet = statement.executeQuery(QUERY_GET_ALL_DATABASES);
		List<String> databases = new ArrayList<>();
		System.out.println("Procurando por bancos de dados...");
		if(except != null) {
			System.out.println("Exceções: " + Arrays.toString(except));
		}
		while(resultSet.next()) {
			synchronized (databases) {
				String datName = resultSet.getString(1);
				if(except != null) {
					for (String datException : except) {
						if(datName.equalsIgnoreCase(datException))
							continue;
					}
				}
				databases.add(resultSet.getString(1));	
			}
		}
		System.out.println("Foi achado os seguintes bancos: ");
		databases.forEach((dbName) -> {
			System.out.println(" - " + dbName);
		});
		return databases;
	}

	private static void updatePostgresWithPort(String port, String pgHomeKey) throws IOException {
		File dataFolder = new File((File) ENV.get(pgHomeKey), "data");
		File postgresqlConfigFile =  checkedFile2(new File(dataFolder, "postgresql.conf"));
		try {
			String originalContent = readFile(postgresqlConfigFile);
			StringBuilder newContentBuilder = new StringBuilder();
			for (String line : originalContent.split("\n")) {
				if(line.startsWith("port"))
					newContentBuilder.append("port = " + port);
				else
					newContentBuilder.append(line);
				newContentBuilder.append('\n');
			}
			writeFile(postgresqlConfigFile, newContentBuilder.toString());
		} catch (Exception e) {
			System.err.println("Houve um erro ao tentar atualizar o arquivo de configuração da versão 9.6.");
			System.err.println("Erro: " + e.getMessage());
			debugStuff();
			System.exit(-1);
		}
	}

	private static synchronized Statement createStatement(String username, String password, String host, String port, String database) 
			throws SQLException, ClassNotFoundException {

		Class.forName("org.postgresql.Driver");
		Connection conn = DriverManager.getConnection("jdbc:postgresql://"+host+":"+port+"/" + database, username, password);
		Statement statement = conn.createStatement();
		return statement;

	}

	private static void stopServices() throws IOException {
		System.out.println("Parando serviços para configuração...");
		for (String serviceNames : getAllServices()) {
			run( String.format("net stop %s", serviceNames), false, (line) -> {
				System.out.println(line);
			});
		}
	}

	public static void startServices() throws IOException {
		System.out.println("Iniciando serviços para migração...");
		int exitCodes = 0;
		for (String serviceNames : getAllServices()) {
			exitCodes += run( String.format("net start %s", serviceNames), false, (line) -> {
				System.out.println(line);
			});
		}
		if(exitCodes != 0) {
			System.err.println("Houve um problema ao subir os serviços.");
			debugStuff();
			System.exit(-1);
		}
	}


	private static void findAllPostgreSQLPaths() throws IOException {
		System.out.println("Procurando arquivos das versões...");
		for (String serviceNames : getAllServices()) {
			run(String.format("(Get-ItemProperty -Path HKLM:SYSTEM\\CurrentControlSet\\Services\\%s).ImagePath", serviceNames), true, (line) -> {

				String[] rows = line.split("\""); /* path at index 1 */

				/* ignorando cabeçalho da resposta do powershell */
				if((line.startsWith("-") && line.endsWith("-")) || line.startsWith("PathName"))
					return;

				/* parent1 = bin, parent2 = pgHome */
				File pgHome = new File(rows[1]).getParentFile()
						.getParentFile();

				if (serviceNames.contains("9.6"))
					ENV.put(NEW_VERSION_HOME, pgHome);
				else
					ENV.put(OLD_VERSION_HOME, pgHome);
			});
		}

		if(ENV.get(NEW_VERSION_HOME) != null || ENV.get(OLD_VERSION_HOME) != null)
			return;

		System.err.println("Não foi possível localizar as pastas das versões instaladas.");
		System.err.println(" * Talvez a versão antiga ou nova não esta registrada como serviço. * ");
		debugStuff();
		System.exit(-1);
	}

	private static void checkingServices() throws IOException {
		System.out.println("Verificando serviços...");
		run("Get-Service \"postgres*\"", true, (line) -> 
		{
			String[] rows = line.split("\\s+");
			String status = rows[0].toUpperCase().trim();
			String name = rows[1].trim();

			/* ignorando cabeçalho da resposta do powershell */
			if(line.startsWith("-") || line.endsWith("-") || name.equalsIgnoreCase("Name") || 
					status.equalsIgnoreCase("Status")) 
				return;

			/* registrando dados dos serviços da versão antiga e nova */
			if (checkForOldVersion(name)) {
				ENV.put(OLD_VERSION_STATE, VersionState.valueOf(status));
				ENV.put(OLD_VERSION_SERVICE_NAME, name);
			} else if (name.contains("9.6")) {
				ENV.put(NEW_VERSION_STATE, VersionState.valueOf(status));
				ENV.put(NEW_VERSION_SERVICE_NAME, name);
			}
		});

		/* checando se foi possível determinar o estado do PostgreSQL 9.0 e 9.6 */
		if (ENV.get(OLD_VERSION_STATE) != VersionState.UNKNOWN && 
				ENV.get(NEW_VERSION_STATE) != VersionState.UNKNOWN &&
				ENV.get(OLD_VERSION_SERVICE_NAME) != null &&
				ENV.get(NEW_VERSION_SERVICE_NAME) != null) {
			return;
		}
		System.err.println("Infelizmente não fui capaz de identificar os serviços corretamente. Por favor, tente novamente.");
		System.err.println(" * Talvez a versão antiga ou nova não esta registrada como serviço. * ");
		debugStuff();
		System.exit(-1);
	}

	private static int run(String command, boolean usePowershell, final Consumer<String> lineConsumer) throws IOException {
		return run(command, usePowershell, lineConsumer, null, null);
	}

	private static int run(String command, boolean usePowershell, final Consumer<String> lineConsumer, Map<String, String> env, File dir) throws IOException {
		Objects.requireNonNull(command, "Comando nulo.");
		ProcessBuilder builder = new ProcessBuilder();
		if(env != null) {
			for(Entry<String, String> entries : env.entrySet()) {
				builder.environment().put(entries.getKey(), entries.getValue());
			}
		}
		if(dir != null) {
			builder.directory(dir);
		}
		if(!usePowershell)
			builder.command("cmd", "/c", command);
		else
			builder.command("cmd", "/c", "powershell" , command);
		builder.redirectErrorStream(true);
		Process process = builder.start();
		process.getOutputStream().close();
		InputStream inputStream = process.getInputStream();
		InputStreamReader insReader = new InputStreamReader(inputStream);
		BufferedReader bufferedreader = new BufferedReader(insReader);
		String line;
		while ((line = bufferedreader.readLine()) != null) {
			if (line.isEmpty()) continue;
			lineConsumer.accept(line);
		}

		InputStream errorStream = process.getErrorStream();
		InputStreamReader insReader1 = new InputStreamReader(errorStream);
		BufferedReader bufferedreader1 = new BufferedReader(insReader1);
		String line1;
		while ((line1 = bufferedreader1.readLine()) != null) {
			if (line1.isEmpty()) continue;
			System.out.println(line1);
		}


		return process.exitValue();
	}

	private static String[] databaseCreationCommands(String databaseName) {
		return new String[]
				{
						"CREATE DATABASE \""+databaseName+"\" WITH OWNER = \"ALTERDATA_ADMIN\" ENCODING = 'UTF8' TABLESPACE = pg_default LC_COLLATE = 'Portuguese_Brazil.1252' LC_CTYPE = 'Portuguese_Brazil.1252' CONNECTION LIMIT = -1;",
						"GRANT CONNECT, TEMPORARY ON DATABASE \""+databaseName+"\" TO public;",
						"GRANT ALL ON DATABASE \""+databaseName+"\" TO \"ALTERDATA_ADMIN\";",
						"GRANT ALL ON DATABASE \""+databaseName+"\" TO \"ALTERDATA_GROUP_"+databaseName+"\";"
				};
	}


	private static List<String> getAllServices() {
		return Arrays.asList(
				(String) ENV.get(OLD_VERSION_SERVICE_NAME), 
				(String) ENV.get(NEW_VERSION_SERVICE_NAME)
				);
	}

	private static boolean checkForOldVersion(String string) {
		return OLD_VERSIONS.stream()
				.anyMatch(version -> string.contains(version));
	}

	private static void writeFile(File file, String content) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		writer.write(content);
		writer.flush();
		writer.close();
	}

	private static String readFile(File file) throws IOException {
		StringBuilder builder = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String line;
			while((line = reader.readLine()) != null) {
				builder.append(line).append('\n');
			}
		}
		return builder.toString().trim();
	}

	private static File checkedFile(File file) {
		if (file == null)
			throw new NullPointerException("Arquivo nulo.");
		else if (!file.exists() || !file.canExecute())
			throw new IllegalStateException("Esse arquivo não pode ser executado ou não existe. [" + file.getAbsolutePath() + "].");
		return file;
	}

	private static File checkedFile2(File file) {
		if (file == null)
			throw new NullPointerException("Arquivo nulo.");
		else if (!file.exists() || !file.canRead())
			throw new IllegalStateException("Esse arquivo não pode ser lido ou não existe. [" + file.getAbsolutePath() + "].");
		return file;
	}

	private static String getAllData() {
		StringBuilder builder = new StringBuilder();
		for (Entry<String, Object> entries : ENV.entrySet()) {
			builder.append(String.format("[KEY: \"%s\", VALUE: \"%s\"]\n", entries.getKey(), entries.getValue()));
		}
		return builder.toString().trim();
	}

	private static void debugStuff() {
		System.err.println("\nDebug (Ambiente): \n\n" + getAllData());
	}
}
