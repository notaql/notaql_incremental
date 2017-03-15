package notaql.extensions.advisor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import notaql.NotaQL;
import notaql.datamodel.AtomValue;
import notaql.datamodel.NumberValue;
import notaql.engines.DatabaseReference;
import notaql.engines.DatabaseSystem;
import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.IncrementalEngine;
import notaql.engines.incremental.snapshot.SnapshotEngine;
import notaql.engines.incremental.snapshot.SnapshotEngineEvaluator;
import notaql.engines.incremental.timestamp.TimestampEngine;
import notaql.engines.incremental.timestamp.TimestampEngineEvaluator;
import notaql.engines.incremental.util.Util;
import notaql.engines.redis.RedisDatabaseReference;
import notaql.engines.redis.RedisEngineEvaluator;
import notaql.extensions.advisor.statistics.exceptions.NoStatisticsException;
import notaql.extensions.dashboard.http.servlets.fromServer.JsonPollFromServerServlet;
import notaql.extensions.notaql.NotaQlWrapper;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.model.vdata.VData;

/**
 * Evaluates a query. Executes it in the fastest possible way. 
 */
public class Advisor {
	/**
	 * Thrown when e.g. a snapshot-based engine evaluator is requested but no snapshot is available.
	 */
	static class UnpreparedEvaluator extends Exception {
		private static final long serialVersionUID = -3873187812616121759L;
	
		public UnpreparedEvaluator() {
	    }
	
	    public UnpreparedEvaluator(String message) {
	        super(message);
	    }
	}
	
	
	// Configuration
	private static final String PROPERTY_ADVISOR_RANDOMNESS = "advisor_randomness";
	private static final double DEFAULT_PERCENTAGE_RANDOMLY_SELECTED_EVALUATORS = 0.1; // Between 0 (no random selection) and 1 (no runtime-based selection)
	private static final boolean ALWAYS_SNAPSHOTS = false; // true = create a snapshot after each execution; false = create sometimes a snapshot (using the random factor) and re-create it if snapshots were selected as the fastest engine
	
	
	// Class variables
	private static Map<String, DatabaseReference> snapshots = new HashMap<String, DatabaseReference>(); // transformation -> snapshot
	private static Map<String, Long> snapshotsCreationTime = new HashMap<String, Long>(); // transformation -> needed time for creating the previous snapshot
	private static Map<String, Long> previousTimestamps = new HashMap<String, Long>(); // transformation -> snapshot
	
	
	/**
	 * Gets the snapshot for the given transformation id (if any is stored).
	 * 
	 * @param transformationId
	 * @return the reference or null
	 */
	private static DatabaseReference getSnapshot(String transformationId) {
		// Redis
		DatabaseReference reference = RedisDatabaseReference.getSnapshots().get(transformationId);
		
		if (reference != null)
			return reference;
		else
			// Other databases
			reference = snapshots.get(transformationId);
		
		return reference;
	}
	
	
	/**
	 * Drops the snapshot for the given transformationId (if there is any).
	 * 
	 * @param transformationId
	 */
	private static void dropSnapshot(String transformationId) {
		try {
			// Redis
			DatabaseReference redisSnapshot = RedisDatabaseReference.getSnapshots().remove(transformationId);
			if (redisSnapshot != null)
				redisSnapshot.drop();
			
			// Other databases
			else {
				DatabaseReference snapshot = snapshots.remove(transformationId);
				if (snapshot != null)
					snapshot.drop();
			}
			
			
			// Clear the needed time for the snapshot
			snapshotsCreationTime.remove(transformationId);

		} catch (IOException e) {
			e.printStackTrace();
			// Not critical
		}
	}
	
	
	/**
	 * Resets all preparations (e.g. informations about the last execution of a query or information about snapshots)
	 * for a specified transformation.
	 * 
	 * Also drops collections, tables and databases used for snapshots.
	 * 
	 * This has to be called when the user by-passes the advisor, because e.g. the timestamp wouldn't contain the
	 * correct value.
	 * 
	 * @param transformationId
	 * @throws IOException 
	 */
	public static void invalidateTransformation(String transformationId) throws IOException {
		// Snapshot
		dropSnapshot(transformationId);
		
		
		// Timestamp
		previousTimestamps.remove(transformationId);
		
		
		JsonPollFromServerServlet.addMessage("Advisor: Transformation '" + transformationId + "' invalidiert");
	}
	
	
	/**
	 * Evaluates a query in the fastest possible way.
	 * 
	 * @param transformation
	 * @throws IOException
	 */
	public static void evaluate(Transformation transformation, String transformationId) throws IOException {
		// Get the parameters
		EngineEvaluator engineEvaluator = transformation.getInEngineEvaluator();
		
		
		// No incremental support? => Full recomputation 
		if (!(engineEvaluator.getEngine() instanceof IncrementalEngine))
			evaluate(transformation, engineEvaluator, transformationId);

		
		// Get the parameters
		EngineEvaluator engineEvaluatorFullRecomputation = getFullRecomputationEvaluator(engineEvaluator);
		Boolean updatesExpected = engineEvaluator.userExpectsUpdates();
		Boolean deletesExpected = engineEvaluator.userExpectsDeletes();
		
		
		// Check the query. Does it only contain SUM() / COUNT() / OBJECT() ? Otherwise => Full recomputation
		if (!isQuerySupportingIncrementalComputation(transformation.getExpression())) {
			JsonPollFromServerServlet.addMessage("Query enthaelt Funktionen, welche nicht inkrementell berechnet werden koennen");
			evaluate(transformation, engineEvaluatorFullRecomputation, transformationId);
		}
		
		
		// Create the engine-evaluators
		EngineEvaluator engineEvaluatorTimestamp = null;
		try { engineEvaluatorTimestamp = getTimestampBasedEvaluator(engineEvaluatorFullRecomputation, transformationId); }
		catch (UnpreparedEvaluator e) { /* Nothing to do here, transformation has to run at least once */ }
		catch (IllegalArgumentException e) { e.printStackTrace(); /* Nothing to do here, engine evaluator has no support for timestamps */ }
		if (engineEvaluatorTimestamp != null 
				&& (!((TimestampEngineEvaluator) engineEvaluatorTimestamp).supportsDeletes() && (deletesExpected == null || deletesExpected)
				|| !((TimestampEngineEvaluator) engineEvaluatorTimestamp).supportsUpdates() && (updatesExpected == null || updatesExpected)))
			engineEvaluatorTimestamp = null;
		
		EngineEvaluator engineEvaluatorSnapshot = null;
		try { engineEvaluatorSnapshot = getSnapshotBasedEvaluator(engineEvaluatorFullRecomputation, transformationId); }
		catch (UnpreparedEvaluator e) { /* Nothing to do here */ }
		catch (IllegalArgumentException e) { /* Nothing to do here, engine evaluator has no support for snapshots */ }
		
		
		// Run the evaluation
		List<EngineEvaluator> engineEvaluatorsSorted = sortEngineEvaluators(transformationId, transformation, new ArrayList<EngineEvaluator>(Arrays.asList(engineEvaluatorFullRecomputation, engineEvaluatorSnapshot, engineEvaluatorTimestamp)));
		EngineEvaluator engineEvaluatorSelected;
		
		for (int i=0; i<engineEvaluatorsSorted.size(); i++) {
			engineEvaluatorSelected = engineEvaluatorsSorted.get(i);
			
			try {
				evaluate(transformation, engineEvaluatorSelected, transformationId);
				break;
			} catch (EvaluationException e) {
				// Probably the timestamp-based evaluator failed, try the next evaluator (no check, because there always will be full recomputation or snapshot)
				e.printStackTrace();
				evaluate(transformation, engineEvaluatorSelected, transformationId);
			} finally {
				// Create the snapshot to be used in the next execution
				createSnapshot(engineEvaluatorSelected, transformationId);
			}
		}
	}
	
	
	/**
	 * Enforces a full recomputation.
	 * 
	 * @param transformation
	 * @param inEngineEvaluator the inEngineEvaluator to be used (or null to leave unchanged)
	 * @throws IOException
	 */
	private static void evaluate(Transformation transformation, EngineEvaluator inEngineEvaluator, String transformationId) throws IOException {
		// Backup of the previously set engine evaluator in the transformation
		EngineEvaluator transformationEngineEvaluator = transformation.getInEngineEvaluator();
		
		try {
			if (inEngineEvaluator != null)
				transformation.setInEngineEvaluator(inEngineEvaluator);
			
			// Status message
			EngineEvaluator inEngine = transformation.getInEngineEvaluator();
			if (inEngine.isBaseType())
				JsonPollFromServerServlet.addMessage("Modus: Volle Neuberechnung");
			else if (inEngine instanceof SnapshotEngineEvaluator)
				JsonPollFromServerServlet.addMessage("Modus: Snapshot-Based");
			else if (inEngine instanceof TimestampEngineEvaluator)
				JsonPollFromServerServlet.addMessage("Modus: Timestamp-Based (ab " + ((TimestampEngineEvaluator) inEngine).getPreviousTimestamp() + ")");
			else
				JsonPollFromServerServlet.addMessage("Unbekannter Modus: " + inEngine);
			
			
			// Store the timestamp
			previousTimestamps.put(transformationId, transformationEngineEvaluator.getCurrentTimestamp());
			
			
			// Execute the transformation
			NotaQlWrapper.evaluateAndCollectStatistics(transformation);
		}
		
		finally {
			transformation.setInEngineEvaluator(transformationEngineEvaluator);
		}
	}
	
	
	/**
	 * Sorts the engine evaluators, best ones are selected as first ones in the list.
	 * 
	 * Steps for the selection:
	 * 1. Filter out evaluators which are null
	 * 2. From the remaining collection: Select the evaluator with the fastest runtime. SOMETIMES select a random engine evaluator so
	 * 	the algorithm can learn the runtimes of other evaluators.
	 * 
	 * @param transformationId
	 * @param transformation
	 * @param engineEvaluators
	 */
	private static List<EngineEvaluator> sortEngineEvaluators(String transformationId, Transformation transformation, List<EngineEvaluator> engineEvaluators) {
		if (engineEvaluators.isEmpty())
			throw new IllegalArgumentException("No EngineEvaluators passed");
		
		
		// Sort out nulls
		Iterator<EngineEvaluator> iterator = engineEvaluators.iterator();
		while (iterator.hasNext()) {
			if (iterator.next() == null)
				iterator.remove();
		}
		
		
		// Check if there are engines remaining
		if (engineEvaluators.isEmpty())
			throw new IllegalStateException("No engine evaluator remaining. Did the Collection contain a full recomputation evaluator?");
		else if (engineEvaluators.size() == 1)
			// No shuffling or sorting needed if the size is 1...
			return engineEvaluators;
		
		
		// Selection
		final double randomness = GET_PERCENTAGE_RANDOMLY_SELECTED_EVALUATORS();
		if (randomness < 1 && (randomness <= 0 || Math.random() >= randomness)) {
			// Backup of the previously set engine evaluator in the transformation
			EngineEvaluator transformationEngineEvaluator = transformation.getInEngineEvaluator();
			try {
				// Select the fastest evaluator
				Map<Long, EngineEvaluator> fastestEngineEvaluators = new TreeMap<Long, EngineEvaluator>();
				
				for (EngineEvaluator engineEvaluator : engineEvaluators) {
					transformation.setInEngineEvaluator(engineEvaluator);

					// Get the statistic
					try {
						long runtimePredictionMillis = NotaQlWrapper.getRuntimePrediction(transformation).runtimeMillis;
						
						// Snapshot based are different because the creation time for the snapshot has to be added
						if (engineEvaluator instanceof SnapshotEngineEvaluator) {
							long neededTimeSnapshot = 0;
							
							if (snapshotsCreationTime.containsKey(transformationId))
								neededTimeSnapshot = snapshotsCreationTime.get(transformationId);
								
							final long totalRuntime = runtimePredictionMillis+neededTimeSnapshot;
							fastestEngineEvaluators.put(totalRuntime, engineEvaluator);
							JsonPollFromServerServlet.addMessage(engineEvaluator.getClass().getSimpleName() + " -> " + totalRuntime + " ms (runtime: " + runtimePredictionMillis + " ms, snapshot: " + neededTimeSnapshot + " ms)");
						}
						else {
							fastestEngineEvaluators.put(runtimePredictionMillis, engineEvaluator);
							JsonPollFromServerServlet.addMessage(engineEvaluator.getClass().getSimpleName() + " -> " + runtimePredictionMillis + " ms");
						}
					}
					catch (NoStatisticsException e) {
						// If there is no statistic the evaluator will be assigned a high priority
						fastestEngineEvaluators.put(-1L, engineEvaluator);
						JsonPollFromServerServlet.addMessage(engineEvaluator.getClass().getSimpleName() + " has no statistic -> high priority");
					} 
				}
				
				
				// Check the runtimes and return
				if (fastestEngineEvaluators.isEmpty())
					throw new IllegalStateException("fastestEngineEvaluators is empty? This should not happen.");
				
				return new ArrayList<EngineEvaluator>(fastestEngineEvaluators.values());				
			} finally {
				transformation.setInEngineEvaluator(transformationEngineEvaluator);
			}
		}
			
			
		// Randomly shuffle the evaluators
		Collections.shuffle(engineEvaluators);
		return engineEvaluators;
	}
	
	
	/**
	 * Transforms a engine evaluator into a snapshot-based engine evaluator
	 * 
	 * @param engineEvaluator
	 * @return
	 * @throws UnpreparedEvaluator 
	 */
	private static EngineEvaluator getSnapshotBasedEvaluator(EngineEvaluator engineEvaluator, String transformationId) throws UnpreparedEvaluator {
		if (engineEvaluator instanceof SnapshotEngineEvaluator)
			return engineEvaluator;
		
		if (!(engineEvaluator.getEngine() instanceof SnapshotEngine))
			throw new IllegalArgumentException("Engine '" + engineEvaluator.getEngine().getEngineName() + "' supports no snapshots");
		
		// Get the snapshot and add the needed arguments to a full recomputation evaluator
		DatabaseReference snapshot = getSnapshot(transformationId);
		if (snapshot == null)
			throw new UnpreparedEvaluator("No snapshot");
		else {
			Map<String, AtomValue<?>> params = getFullRecomputationEvaluatorParams(engineEvaluator);
			params.put(((SnapshotEngine) engineEvaluator.getEngine()).getSnapshotParameterName(), ((SnapshotEngine) engineEvaluator.getEngine()).getSnapshotValue(snapshot));
			return engineEvaluator.getEngine().createEvaluator(engineEvaluator.getParser(), params);
		}
	}
	
	
	/**
	 * Transforms a engine evaluator into a timestamp-based engine evaluator
	 * 
	 * @param engineEvaluator
	 * @return
	 * @throws UnpreparedEvaluator 
	 */
	private static EngineEvaluator getTimestampBasedEvaluator(EngineEvaluator engineEvaluator, String transformationId) throws UnpreparedEvaluator {
		if (engineEvaluator instanceof TimestampEngineEvaluator)
			return engineEvaluator;
		
		if (!(engineEvaluator.getEngine() instanceof TimestampEngine))
			throw new IllegalArgumentException("Engine '" + engineEvaluator.getEngine().getEngineName() + "' supports no timestamps");
		
		// Get the previous timestamp and add the needed arguments to a full recomputation evaluator
		Long previousTimestamp = previousTimestamps.get(transformationId);	
		if (previousTimestamp == null)
			throw new UnpreparedEvaluator("No timestamp");	
		else {
			Map<String, AtomValue<?>> params = getFullRecomputationEvaluatorParams(engineEvaluator);
			params.put(((TimestampEngine) engineEvaluator.getEngine()).getTimestampParameterName(), new NumberValue(previousTimestamp));
			return engineEvaluator.getEngine().createEvaluator(engineEvaluator.getParser(), params);
		}
	}
	
	
	/**
	 * Transforms a engine evaluator into its base type.
	 * 
	 * @param engineEvaluator
	 * @return
	 */
	private static EngineEvaluator getFullRecomputationEvaluator(EngineEvaluator engineEvaluator) {
		if (engineEvaluator.isBaseType())
			return engineEvaluator;
		
		return engineEvaluator.getEngine().createEvaluator(engineEvaluator.getParser(), getFullRecomputationEvaluatorParams(engineEvaluator));
	}
	
	
	/**
	 * Removes all arguments from an engine evaluator which are not required.
	 * 
	 * @param engineEvaluator
	 * @return
	 */
	private static Map<String, AtomValue<?>> getFullRecomputationEvaluatorParams(EngineEvaluator engineEvaluator) {
		Map<String, AtomValue<?>> params = new HashMap<String, AtomValue<?>>();
		
		for (Entry<String, AtomValue<?>> entry : engineEvaluator.getParams().entrySet()) {
			if (engineEvaluator.getEngine().getRequiredArguments().contains(entry.getKey()))
				params.put(entry.getKey(), entry.getValue());
		}		
		
		return params;
	}
	
	
	/**
	 * Creates a snapshot for the given engine-evaluator.
	 * 
	 * The snapshot is only created if the previous engineEvaluator was snapshot-based (snapshot shall probably
	 * be used again next time because it was selected) or if the user wants to create snapshots always or if
	 * by chance (so the Advisor is able to learn if snapshots are the fastest possible way).
	 * 
	 * @param engineEvaluator the engine evaluator used for the previous evaluation
	 * @param transformationId
	 * @throws IOException 
	 */
	private static void createSnapshot(EngineEvaluator engineEvaluator, String transformationId) {
		final double randomness = GET_PERCENTAGE_RANDOMLY_SELECTED_EVALUATORS();
		if (ALWAYS_SNAPSHOTS || engineEvaluator instanceof SnapshotEngineEvaluator || (randomness > 0 && (randomness >= 1 || Math.random() < randomness))) {
			// Create a snapshot
			JsonPollFromServerServlet.addMessage("Erstelle Snapshot");
			
			
			// Check the input
			if (!(engineEvaluator instanceof DatabaseSystem))
				throw new IllegalArgumentException("Engine '" + engineEvaluator.getEngine().getEngineName() + "' supports no snapshot-creation");
			
			
			// Create the snapshot
			try {
				final long starttime = System.currentTimeMillis();
				DatabaseReference snapshot = ((DatabaseSystem) engineEvaluator).getDatabaseReference().createSnapshot(((DatabaseSystem) engineEvaluator).getDatabaseReference(), transformationId);
				final long neededTimeMillis = System.currentTimeMillis() - starttime;
			
				// Store the reference to the snapshot
				if (!(engineEvaluator instanceof RedisEngineEvaluator)) {
					if (snapshot != null)
						snapshots.put(transformationId, snapshot);
					else
						throw new IllegalStateException("Snapshot is null");
				}
				
				// Store the needed time
				snapshotsCreationTime.put(transformationId, neededTimeMillis);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		else {
			// Dont create a snapshot... But drop the tables so the reference can be deleted safely
			JsonPollFromServerServlet.addMessage("Erstelle keinen Snapshot");
			dropSnapshot(transformationId);
		}
	}
	
	
	/**
	 * Checks if the query contains only functions which support incremental computations (SUM() / COUNT() / OBJECT()).
	 * 
	 * @param queryExpression
	 * @return
	 */
	private static boolean isQuerySupportingIncrementalComputation(VData queryExpression) {
		return Util.isQuerySupportingIncrementalComputation(queryExpression);
	}
	
	
	/**
	 * @return the learning-factor for the Advisor 
	 */
	private static double GET_PERCENTAGE_RANDOMLY_SELECTED_EVALUATORS() {
		String configurationValue = NotaQL.getProperties().getProperty(PROPERTY_ADVISOR_RANDOMNESS);
		
		if (configurationValue != null)
			return Double.valueOf(configurationValue);
		else
			return DEFAULT_PERCENTAGE_RANDOMLY_SELECTED_EVALUATORS;
	}
}
