
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 *
 * @author Charles Masson
 */
public class ETL {

    private static final int N_GEO = 578, N_MISC = 6, N_TYPE = 5, N_NATURE = 5, N_TEMPS = 36;
    private static final int INITIAL_CONTRACT_BUCKET_SIZE = 16, INITIAL_CLIENT_BUCKET_SIZE = 16;
    private static final int N = N_GEO * N_MISC * N_TYPE * N_NATURE * N_TEMPS;

    private byte[] clientMisc, clientType, contractMisc, contractNature, contractType;
    private double[] invoiceAmounts;
    private int[] clientDistinctCounts, contractClient, contractDistinctCounts, contractIndex, invoiceCounts;
    private short[] clientGeo, contractGeo;
    private int[][] clientBuckets, contractBuckets;
    private long[] invoiceConsumptions;

    public void loadClients(File file) throws FileNotFoundException, IOException {

	// Calculating the number of clients
	int maxClientId = 0;
	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	    br.readLine();
	    String line;
	    while ((line = br.readLine()) != null)
		maxClientId = Math.max(maxClientId, Integer.valueOf(line.split(",")[0]));
	}

	clientType = new byte[maxClientId + 1];
	clientMisc = new byte[maxClientId + 1];
	clientGeo = new short[maxClientId + 1];

	// Reading clients from file
	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	    br.readLine();
	    String line;
	    String[] tmp;
	    int id;
	    while ((line = br.readLine()) != null) {
		tmp = line.split(",");
		id = Integer.valueOf(tmp[0]);
		clientType[id] = Byte.valueOf(tmp[1]);
		clientGeo[id] = Short.valueOf(tmp[2]);
		clientMisc[id] = Byte.valueOf(tmp[3]);
		id++;
	    }
	}
    }

    public void loadContractsAndJoinToClients(File file) throws FileNotFoundException, IOException {

	// calculating the number of contracts
	int maxContractId = 0;
	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	    br.readLine();
	    String line;
	    while ((line = br.readLine()) != null)
		maxContractId = Math.max(maxContractId, Integer.valueOf(line.split(",")[0]));
	}

	contractClient = new int[maxContractId + 1];
	contractType = new byte[maxContractId + 1];
	contractMisc = new byte[maxContractId + 1];
	contractGeo = new short[maxContractId + 1];
	contractNature = new byte[maxContractId + 1];
	contractIndex = new int[maxContractId + 1];

	// Reading contracts from file and joining to clients
	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	    String line;
	    String[] tmp;
	    int id, client;
	    br.readLine();
	    while ((line = br.readLine()) != null) {
		tmp = line.split(",");
		id = Integer.valueOf(tmp[0]);
		client = Integer.valueOf(tmp[1]);
		contractClient[id] = client;
		contractNature[id] = Byte.valueOf(tmp[2]);
		contractType[id] = clientType[client];
		contractGeo[id] = clientGeo[client];
		contractMisc[id] = clientMisc[client];
		contractIndex[id] = ((((contractGeo[id] - 1) * N_TYPE + contractType[id] - 1) * N_MISC + contractMisc[id] - 1) * N_NATURE + contractNature[id] - 1) * N_TEMPS;
	    }
	}
    }

    public void processInvoicesFromBinFile(File file, int numberThreadPools, int numberThreads, int chunkSize) throws FileNotFoundException, IOException {

	int[] localInvoiceCounts = new int[N];
	long[] localInvoiceConsumption = new long[N];
	double[] localInvoiceAmount = new double[N];
	contractBuckets = new int[N][INITIAL_CONTRACT_BUCKET_SIZE];

	try (BufferedInputStream dis = new BufferedInputStream(new FileInputStream(file))) {
	    
	    Runnable r = () -> {
		
		byte time;
		float amount;
		int contract, numberReadBytes = 0, i, j, k;
		short consumption;
		byte[] chunk = new byte[chunkSize];
		double[] threadInvoiceAmounts = new double[N];
		int[] threadInvoiceCounts = new int[N];
		long[] threadInvoiceConsumption = new long[N];
		ByteBuffer bb;
		
		try {
		    while (numberReadBytes != -1) {
						
			// reading a chunk from invoice file
			synchronized (dis) {
			    numberReadBytes = dis.read(chunk);
			}
			bb = ByteBuffer.wrap(chunk);
			
			// processing the invoices in the chunk
			for (i = 0; i < numberReadBytes / 16; i++) {
			    
			    bb.getInt();
			    contract = bb.getInt();
			    time = bb.get();
			    amount = bb.getFloat();
			    consumption = bb.getShort();
			    bb.get();
			    k = contractIndex[contract] + time - 1;
			    
			    threadInvoiceCounts[k]++;
			    threadInvoiceConsumption[k] += consumption;
			    threadInvoiceAmounts[k] += amount;
			    
			    // adding the contract id to the right bucket
			    j = 0;
			    synchronized (contractBuckets[k]) {
				while (true) {
				    if (j >= contractBuckets[k].length) {
					contractBuckets[k] = Arrays.copyOf(contractBuckets[k], 2 * contractBuckets[k].length + 1);
					contractBuckets[k][j] = contract;
					break;
				    } else if (contractBuckets[k][j] == 0) {
					contractBuckets[k][j] = contract;
					break;
				    } else if (contractBuckets[k][j] == contract)
					break;
				    j++;
				}
			    }
			}
		    }
		} catch (IOException ex) {
		    Logger.getLogger(ETL.class.getName()).log(Level.SEVERE, null, ex);
		}

		synchronized (localInvoiceCounts) {
		    for (int l = 0; l < N; l++)
			localInvoiceCounts[l] += threadInvoiceCounts[l];
		}
		synchronized (localInvoiceConsumption) {
		    for (int l = 0; l < N; l++)
			localInvoiceConsumption[l] += threadInvoiceConsumption[l];
		}
		synchronized (localInvoiceAmount) {
		    for (int l = 0; l < N; l++)
			localInvoiceAmount[l] += threadInvoiceAmounts[l];
		}
	    };
	    
	    // Launching the threads
	    ExecutorService executor = Executors.newFixedThreadPool(numberThreadPools);
	    Set<Future<?>> futures = new HashSet<>();
	    for (int i = 0; i < numberThreads; i++)
		futures.add(executor.submit(r));
	    
	    // Waiting for the threads to terminate
	    try {
		for (Future<?> f : futures)
		    f.get();
	    } catch (InterruptedException | ExecutionException ex) {
		Logger.getLogger(ETL.class.getName()).log(Level.SEVERE, null, ex);
	    }
	    executor.shutdown();
	}
	
	invoiceCounts = localInvoiceCounts;
	invoiceConsumptions = localInvoiceConsumption;
	invoiceAmounts = localInvoiceAmount;
    }

    public void calculateContractDistinctCount() {
	contractDistinctCounts = new int[N];
	IntStream.range(0, N).forEach((bucket) -> {
	    int count = 0;
	    while (count < contractBuckets[bucket].length && contractBuckets[bucket][count] != 0)
		count++;
	    contractDistinctCounts[bucket] = count;
	});
    }

    public void calculateClientDistinctCount() {
	clientBuckets = new int[N][INITIAL_CLIENT_BUCKET_SIZE];
	clientDistinctCounts = new int[N];
	IntStream.range(0, N).forEach((bucket) -> {
	    int[] contractBucket = contractBuckets[bucket], clientBucket = clientBuckets[bucket];
	    int client, i = 0;
	    for (int j = 0; j < contractBucket.length && contractBucket[j] != 0; j++) {
		client = contractClient[contractBucket[j]];
		while (true) {
		    if (i >= clientBucket.length) {
			clientBucket = Arrays.copyOf(clientBucket, 2 * clientBucket.length + 1);
			clientBucket[i] = client;
			break;
		    } else if (clientBucket[i] == 0) {
			clientBucket[i] = client;
			break;
		    } else if (clientBucket[i] == client)
			break;
		    i++;
		}
	    }
	    i = 0;
	    while (i < clientBuckets[bucket].length && clientBuckets[bucket][i] != 0)
		i++;
	    clientDistinctCounts[bucket] = i;
	});
    }

    public void writeHypercubeToFile(File file) throws FileNotFoundException, IOException {
	DecimalFormat df = new DecimalFormat("#.00");
	try (PrintWriter pw = new PrintWriter(new BufferedOutputStream(new FileOutputStream(file)))) {
	    int id;
	    pw.println("geo,type,misc,nature,time,consumption,amount,nclients,ncontrats,ninvoices");
	    for (short geo = 1; geo <= N_GEO; geo++)
		for (byte type = 1; type <= N_TYPE; type++)
		    for (byte misc = 1; misc <= N_MISC; misc++)
			for (byte nature = 1; nature <= N_NATURE; nature++)
			    for (byte temps = 1; temps <= N_TEMPS; temps++) {
				id = ((((geo - 1) * N_TYPE + type - 1) * N_MISC + misc - 1) * N_NATURE + nature - 1) * N_TEMPS + temps - 1;
				if (invoiceCounts[id] != 0)
				    pw.println(geo + "," + type + "," + misc + "," + nature + "," + temps + "," + invoiceConsumptions[id] + "," + df.format((double) Math.round(100 * invoiceAmounts[id]) / 100) + ","
					    + clientDistinctCounts[id] + "," + contractDistinctCounts[id] + "," + invoiceCounts[id]);
			    }
	}
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {

	Map<String, String> argMap = new HashMap<>();

	for (int i = 2; i < args.length; i += 2)
	    argMap.put(args[i].substring(1), args[i + 1]);

	// Decoding args
	File dataFolder = new File(args[0]);
	File outputFile = new File(args[1]);
	int numberThreads = argMap.containsKey("t") ? Integer.valueOf(argMap.get("t")) : Runtime.getRuntime().availableProcessors();
	int numberThreadPools = argMap.containsKey("p") ? Integer.valueOf(argMap.get("p")) : numberThreads;
	int chunkSize = argMap.containsKey("s") ? Integer.valueOf(argMap.get("s")) : 16384;
	int logType = argMap.containsKey("l") ? Integer.valueOf(argMap.get("l")) : 0;

	if (chunkSize % 16 != 0) {
	    System.out.println("Wrong chunk size");
	    return;
	}
	
	File clientFile = new File(dataFolder, "clients.csv");
	File contractFile = new File(dataFolder, "contracts.csv");
	File invoiceFile = new File(dataFolder, "invoices.bin");

	// Processing the data
	long[] times = new long[6];

	long startTime = System.currentTimeMillis(), intermediateTime;

	ETL etl = new ETL();

	if (logType == 0)
	    System.out.println("Configuration: " + numberThreadPools + " thread pool" + (numberThreadPools > 1 ? "s" : "")
		    + ", " + numberThreads + " thread" + (numberThreads > 1 ? "s" : "") + ", chunk size: " + chunkSize + "B");

	if (logType == 0)
	    System.out.print("Loading clients");
	intermediateTime = System.currentTimeMillis();
	etl.loadClients(clientFile);
	times[0] = System.currentTimeMillis() - intermediateTime;
	if (logType == 0)
	    System.out.println(" - " + times[0] + "ms");

	if (logType == 0)
	    System.out.print("Loading contracts and joining to clients");
	intermediateTime = System.currentTimeMillis();
	etl.loadContractsAndJoinToClients(contractFile);
	times[1] = System.currentTimeMillis() - intermediateTime;
	if (logType == 0)
	    System.out.println(" - " + times[1] + "ms");

	if (logType == 0)
	    System.out.print("Processing invoices from file");
	intermediateTime = System.currentTimeMillis();
	etl.processInvoicesFromBinFile(invoiceFile, numberThreadPools, numberThreads, chunkSize);
	times[2] = System.currentTimeMillis() - intermediateTime;
	if (logType == 0)
	    System.out.println(" - " + times[2] + "ms");

	if (logType == 0)
	    System.out.print("Calculating count distinct contracts");
	intermediateTime = System.currentTimeMillis();
	etl.calculateContractDistinctCount();
	times[3] = System.currentTimeMillis() - intermediateTime;
	if (logType == 0)
	    System.out.println(" - " + times[3] + "ms");

	if (logType == 0)
	    System.out.print("Calculating count distinct clients");
	intermediateTime = System.currentTimeMillis();
	etl.calculateClientDistinctCount();
	times[4] = System.currentTimeMillis() - intermediateTime;
	if (logType == 0)
	    System.out.println(" - " + times[4] + "ms");

	if (logType == 0)
	    System.out.print("Writing hypercube to file");
	intermediateTime = System.currentTimeMillis();
	etl.writeHypercubeToFile(outputFile);
	times[5] = System.currentTimeMillis() - intermediateTime;
	if (logType == 0)
	    System.out.println(" - " + times[5] + "ms");

	if (logType == 0)
	    System.out.println("Total time: " + (System.currentTimeMillis() - startTime) + "ms");

	if (logType == 1)
	    System.out.println(numberThreadPools + "," + numberThreads + "," + chunkSize + "," + times[0] + "," + times[1] + "," + times[2] + "," + times[3] + "," + times[4] + "," + times[5]);
    }
}
