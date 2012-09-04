package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.PriorityQueue;

import de.tu_berlin.dima.aim3.querysuggestion.Session;
import de.tu_berlin.dima.aim3.querysuggestion.TimeUtils;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

public class SessionReducer extends ReduceStub {

	// TODO change to config parameter
	/** maximum interval of two queries for being in one session */
	private static final int MAX_SEC_INTERVAL = 600;

	private static final Object EMPTY_QUERY = null;

	private final PactRecord outputRecord = new PactRecord();

	private final PactString query = new PactString();

	private final PactString sessionId = new PactString("default");

	private final PactString ref = new PactString();

	private final PactString doc = new PactString();

	private final PactInteger oneCount = new PactInteger(1);

	/** number of emitted sessions to create unique session Ids */
	private int sessionCount = 0;

	private static final List<String> FILTER_QUERY = Arrays.asList("mars");

	private static final boolean USE_FILTER = false;

	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {

		// TODO change when sec sort is available

		// // buffer for sorting records by time
		// PriorityQueue<PactRecord> logEntries = new
		// PriorityQueue<PactRecord>(10, new Comparator<PactRecord>() {
		//
		// @Override
		// public int compare(PactRecord record1, PactRecord record2) {
		//
		// // get time values from records
		// long time1 = record1.getField(1, PactLong.class).getValue();
		// long time2 = record2.getField(1, PactLong.class).getValue();
		// // compare times
		// if (time1 < time2) {
		// return -1;
		// }
		// if (time1 > time2) {
		//
		// return 1;
		// }
		// return 0;
		// }
		// });
		//
		// // buffer values sorted for log entries, sorting by time taken from
		// // record
		// // PactRecord record = null;
		// while (records.hasNext()) {
		// PactRecord record = records.next();
		// // copy everything!
		// PactRecord copy = record.createCopy();
		//
		// logEntries.add(copy);
		//
		// }
		// // //// ////// ////// ////// ////// //////
		// String lastQuery = "";
		// String lastUrl = "";
		// long lastTime = 0;
		//
		// // TODO change to handle sessions with equal starting queries
		// // maybe first iterate over all entries and remove ignored ones
		//
		// // all active sessions
		// // List<Session> activeSessions = new ArrayList<Session>();
		// Map<String, Session> activeSessionsMap = new HashMap<String,
		// Session>();
		//
		// // get first entry to read userId only
		// PactRecord logEntry = logEntries.peek();
		// String userId = logEntry.getField(0, PactInteger.class).toString();
		//
		// // iterate over log entries and build sessions
		// while (logEntries.size() != 0) {
		// // System.out.println(logEntry.getField(2,
		// // PactString.class).getValue());
		// logEntry = logEntries.remove();

		// TODO from here no change after sec sort

		String lastQuery = "";
		String lastUrl = "";
		long lastTime = 0;
		String userId = "";

		// all active sessions
		Map<String, Session> activeSessionsMap = new HashMap<String, Session>();
		PactRecord logEntry = null;
		while (records.hasNext()) {
			logEntry = records.next();
			// parse entry
			userId = logEntry.getField(0, PactInteger.class).toString();
			long time = logEntry.getField(1, PactLong.class).getValue();
			String query = logEntry.getField(3, PactString.class).getValue();
			String doc = logEntry.getField(5, PactString.class).getValue();

			// System.out.println("from que: " + time + " " + query + " " +
			// doc);

			// initialize time
			if (lastTime == 0) {
				lastTime = time;
			}

			// TODO don't ignore if big time in between
			// ignore if two following entries have same query and url
			if (!(query.equals(lastQuery) && doc.equals(lastUrl))) {

				// System.out.println("USE |" + query + " " + time + " " +
				// url);// +
				// timeDiffInSec("",
				// ""));

				// get key for all active sessions
				Iterator<String> sessionIter = activeSessionsMap.keySet()
						.iterator();
				Session session;
				String sessionQuery = "";
				// while there are active session
				while (sessionIter.hasNext()) {
					sessionQuery = sessionIter.next();
					session = activeSessionsMap.get(sessionQuery);
					// check if time difference between queries is in allowed
					// range
					if (TimeUtils.timeDiffInSec(session.getTime(), time) <= MAX_SEC_INTERVAL) {
						// TODO change
						// only add refinement if it is different from query
						if (!session.getQuery().equals(query)) {
							// add refinement and clicked url to this session
							session.addSessionEntry(query, doc);
							// System.out.println("add to " + sessionQuery +
							// " r: " + query + " d: " +doc);
						}
					} else {

						// System.out.println("Time emit q: " +
						// session.getQuery());

						// session is complete emit it
						emitSession(userId, session, out);
						// than delete session from active session list
						sessionIter.remove();
						// activeSessionsMap.remove(sessionQuery);
					}
				}

				/** adding new active session */
				// only add new active session if query term changed
				if (!query.equals(lastQuery)) {

					// add new active session
					// activeSessions.add(new Session(query, time));

					// add new active session
					// TODO re-think: only add if no active session for this
					// query
					// exists
					// if (query.equals("mars") || query.equals("jaguar") ||
					// query.equals("internet") ||
					// query.equals("irak")) {
					if (!USE_FILTER || FILTER_QUERY.contains(query)) {
						if (!activeSessionsMap.containsKey(query)) {
							activeSessionsMap.put(query, new Session(query,
									time));
						}
					}
				}
			} else {
				// System.out.println("IGN |" + query + " " + time + " " +
				// url);// +
				// timeDiffInSec("",
			}

			// save last query and url
			lastQuery = query;
			lastUrl = doc;
			lastTime = time;

		}
		// emit rest of active sessions
		for (Session session : activeSessionsMap.values()) {
			emitSession(userId, session, out);

			// for (Session session : activeSessions) {
			// emitSession(userId, session, out);

			// System.out.println(session.getQuery() + " || "+
			// session.getRefinements());
		}

	}

	/**
	 * Emit contents of a session if it is valid.
	 * 
	 * A valid session contains at least one refinement with a doc or two
	 * refinements.
	 * 
	 * @param session
	 * @param out
	 */
	private void emitSession(String userId, Session session,
			Collector<PactRecord> out) {

		boolean validSession = false;

		// only emit if refinement list not empty
		int sessionRefineCount = session.getRefinements().size();
		// exclude sessions without refinement
		if (sessionRefineCount > 0) {
			// exclude session with only one refinement and no doc
			if (!(sessionRefineCount == 1 && session.getDocs().get(0)
					.equals(EMPTY_QUERY))) {
				validSession = true;
			}
		}

		// emit session content
		if (validSession) {
			/** set fields that are the same for all members of a session */

			// create session id for later join
			sessionId.setValue(userId + "_" + sessionCount++);
			query.setValue(session.getQuery());

			outputRecord.setField(2, sessionId);
			outputRecord.setField(3, query);

			// TODO don't double emit equal entries without doc, because co occ
			// is only once per session
			// but doc count is absolute

			/** set all refinements and docs and emit session entry */
			for (int i = 0; i < sessionRefineCount; i++) {
				ref.setValue(session.getRefinements().get(i));
				doc.setValue(session.getDocs().get(i));
				outputRecord.setField(4, ref);
				outputRecord.setField(5, doc);
				outputRecord.setField(6, oneCount);

				// emit valid log entry
				out.collect(this.outputRecord);
				// System.out.println("Ses: " + sessionId.getValue() + " " +
				// query.getValue() + "\t|\t" +
				// ref.getValue() + " "
				// + doc.getValue());

			}
		}

	}

}
