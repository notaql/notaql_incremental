package notaql.extensions.advisor.statistics.container;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import notaql.NotaQL;
import notaql.extensions.advisor.statistics.Statistic;
import notaql.extensions.notaql.Configuration;

/**
 * Stores the statistics in a file.
 */
public class StatisticsContainerFile extends StatisticsContainerInMemory {
	public class AppendObjectOutputStream extends ObjectOutputStream {
		public AppendObjectOutputStream(OutputStream outputStream) throws IOException {
			super(outputStream);
		}

		/* (non-Javadoc)
		 * @see java.io.ObjectOutputStream#writeStreamHeader()
		 */
		@Override
		protected void writeStreamHeader() throws IOException {
			reset();
		}
	}
	
	
	// Configuration
	private static final long serialVersionUID = 5819518137185429324L;
	private static final String PROPERTY_FILE_PATH = "statistics_file";
	
	
	// Object variables
	private final Path path;
	private ObjectOutputStream objectOutputStream;
	

	public StatisticsContainerFile(Configuration configuration) throws IOException {
		this(configuration, NotaQL.getProperties().getProperty(PROPERTY_FILE_PATH));
	}

	public StatisticsContainerFile(Configuration configuration, String path) {
		this(configuration, FileSystems.getDefault().getPath(path));
	}

	public StatisticsContainerFile(Configuration configuration, Path path) {
		super(configuration);
		this.path = path;
		
		try {
			this.loadFromFile();
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
			throw new IllegalStateException("Could not load the statistics: " + e.getMessage());
		}
	}
	
	
	/**
	 * Loads all stored statistics.
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void loadFromFile() throws IOException, ClassNotFoundException {
		if (path.toFile().exists()) {
			FileInputStream fileInputStream = new FileInputStream(path.toFile());
			ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
			try {
				Statistic loadedStatistic;
				while ((loadedStatistic = (Statistic) objectInputStream.readObject()) != null)
					super.addStatistic(loadedStatistic);
			} catch (EOFException e) {}
			
			fileInputStream.close();
		}
	}
	
	
	/**
	 * Appends a statistic to the file.
	 * 
	 * @param statistic
	 * @throws IOException
	 */
	private void storeToFile(Statistic statistic) throws IOException {
		if (objectOutputStream == null) {
			if (path.toFile().exists())
				objectOutputStream = new AppendObjectOutputStream(new FileOutputStream(path.toFile(), true));
			else
				objectOutputStream = new ObjectOutputStream(new FileOutputStream(path.toFile(), true));				
		}
	    
	    objectOutputStream.writeObject(statistic);
	    objectOutputStream.flush();
	}
	

	/* (non-Javadoc)
	 * @see notaql.extensions.advisor.statistics.container.StatisticsContainerInMemory#addStatistic(notaql.extensions.advisor.statistics.Statistic)
	 */
	@Override
	public boolean addStatistic(Statistic statistic) {
		boolean returnValue = super.addStatistic(statistic);
		
		try {
			this.storeToFile(statistic);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return returnValue;
	}

}
