package com.dp.miniDBMS.sm.smm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.dp.miniDBMS.cm.TransactionManager;
import com.dp.miniDBMS.sm.smd.DiskManager;
import com.dp.miniDBMS.sm.smm.BPlusTree.RangePolicy;
import com.dp.miniDBMS.sm.smo.Convert;
import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.DicTable;
import com.dp.miniDBMS.sm.smo.Page;
import com.dp.miniDBMS.sm.smo.RID;
import com.dp.miniDBMS.sm.smo.Tuple;
import com.dp.miniDBMS.sm.smo.type.TypeBase;
import com.dp.miniDBMS.sm.smo.type.TypeEnum;
import com.dp.miniDBMS.sm.smu.GlobalConst;

/**
 *
 * @날짜 : Aug 20, 2020
 * @작성자 : 박선희
 * @클래스설명 : in-memory tbs 관리 하는 페이지
 *
 */

public class MemoryTBS implements GlobalConst {
	private byte[][] tbsData; // 데이터를 담아놓는 버퍼
	private int fid; // memory tbs manager로 부터 할당된 파일 아이디
	private int pageEndNum = -1;
	private int lastPos = -1;

	private List<DicColumn> columns; // attribute 속성 리스트
	private List<TypeBase> attrTypes = new ArrayList<TypeBase>(); // attribute 타입 리스트
	private DicTable table;

	private HashMap<String, Integer> tableInfo = new HashMap<String, Integer>(); // key : 칼럼 이름 , value : 몇 번째 칼럼인지
	private HashMap<String, BPlusTree> btreeInfo = new HashMap<String, BPlusTree>(); // key : 칼럼 이름, value : 칼럼의 b plus
																						// tree
	private int index = 0; // 인덱스 컬럼

	DiskManager disk = new DiskManager();
	String fileName;

	private List<RID> ridList = new ArrayList<RID>();
	private List<Integer> dirtyPageIdList = new ArrayList<Integer>(); // 새로 써야할 rid
	public LinkedList<Integer> deleteIntList = new LinkedList<Integer>();
	public int numRecord = -1;

	private Object mutex = new Object();

	private List<Tuple> tuples = new LinkedList<Tuple>();

	public MemoryTBS(int pageCnt, int fid, DicTable table) {
		this.table = table;
		this.columns = table.getColumnList();

		int i = 0; // 인덱스 순서
		for (DicColumn dt : columns) {
			tableInfo.put(dt.getColName(), i++);
			attrTypes.add(dt.getType());
		}

		this.tbsData = new byte[pageCnt][PAGE_SIZE];
		this.fid = fid;

		this.fileName = GlobalConst.DATA_FILEPATH + this.table.getDatabaseName() + "." + table.getTableName() + ".dat";
		this.index = this.table.indexIdx;

		try {
			disk.openDB(table); // 파일 열기
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public boolean bulkLoad() {
		BPlusTree<Integer> bpt = new BPlusTree<Integer>(4);

		this.pageEndNum = disk.allReadFile(tbsData);

		if (this.pageEndNum == -1) {
			this.pageEndNum = 1; // 맨 처음 튜플
			this.lastPos = 0;
			return true;
		}

		int colCnt = columns.size(); // 컬럼의 갯수

		int i = 0;
		int pos = 0;

		int len = table.getLen();

		int size = 0;
		int tupleSize = Convert.getIntValue(pos, tbsData[i]);
		// table.setLen(Convert.getIntValue(pos+4, tbsData[i]));

		i++;
		
		while (size < tupleSize) {
			// 페이지 넘기기
			if (pos + len >= PAGE_SIZE) {
				pos = 0;
				i++;
			}

			List<TypeBase> oneTuple = new ArrayList<TypeBase>();

			int firstI = i; // tuple 의 맨 처음 페이지 위치
			int firstPos = pos;
			for (int j = 0; j < colCnt; j++) {
				TypeBase typeTemp = attrTypes.get(j);
				TypeEnum enumTemp = typeTemp.type;

				// 추가할 값
				TypeBase attr = enumTemp.makeType();

				if (enumTemp == TypeEnum.INT) {
					oneTuple.add(TypeEnum.INT.makeTypeWithValue(Convert.getIntValue(pos, tbsData[i])));
					// System.out.println(Convert.getIntValue(pos, tbsData[i]));

				} else if (enumTemp == TypeEnum.CHAR) {
					oneTuple.add(TypeEnum.CHAR
							.makeTypeWithValue(Convert.getStringValue(pos, tbsData[i], typeTemp.getMaxSize())));
					// System.out.println(Convert.getStringValue(pos, tbsData[i],
					// typeTemp.getMaxSize()));
				}
				pos += typeTemp.getMaxSize();

			}
			Tuple temp = new Tuple(oneTuple, new RID(fid, firstI, firstPos, size++));
			tuples.add(temp);
			bpt.insert(Integer.parseInt(oneTuple.get(this.index).getValue().toString()), temp);
			++numRecord;
		}
		this.lastPos = pos;
		btreeInfo.put("C1", bpt);
		return true;
	}

	public void flushDirtyPages() {
		synchronized (mutex) {
			if (deleteIntList.size() != 0) {
				numRecord -= deleteIntList.size();
				deleteCheckpointPrepare();
			}

			TreeSet<Integer> dirtyPage = new TreeSet<Integer>(); // flush할 page id
			int colCnt = columns.size(); // 컬럼의 갯수

			for (int ridIdx : dirtyPageIdList) {
				Tuple temp = tuples.get(ridIdx);

				int rid = temp.rid.tuplePos;
				// System.out.println("checkpointing " + rid);

				int pageid = temp.rid.pageID;
				int pageoffset = temp.rid.offset;

				dirtyPage.add(pageid);

				for (int j = 0; j < colCnt; j++) { // 컬럼 별로 읽기
					TypeBase typeTemp = attrTypes.get(j);
					TypeEnum enumTemp = typeTemp.type;

					if (enumTemp == TypeEnum.INT) {

						Convert.setIntValue(Integer.parseInt(temp.getValues().get(j).getValue().toString()), pageoffset,
								tbsData[pageid]);
					} else if (enumTemp == TypeEnum.CHAR) {
						Convert.setStringValue(temp.getValues().get(j).getValue().toString(), pageoffset,
								tbsData[pageid], typeTemp.getMaxSize());
						// System.out.println(Convert.getStringValue(pageoffset, tbsData[pageid],
						// typeTemp.getMaxSize()));

					}
					pageoffset += typeTemp.getMaxSize();
				}
			}
			dirtyPageIdList.clear(); // 더티페이지 리셋
			Iterator iter = dirtyPage.iterator(); // Iterator 사용

			// header 기록 -> numrecord , byte length
			Convert.setIntValue(numRecord + 1, 0, tbsData[0]);
			Convert.setIntValue(table.getLen(), 4, tbsData[0]);

			disk.write_page(new Page(0, tbsData[0]));
			while (iter.hasNext()) { // 값이 있으면 true 없으면 false
				int pid = (Integer) iter.next();

				disk.write_page(new Page(pid, tbsData[pid]));
			}
			System.out.println("dirty page checkpoint end!");
		}
	}

//	public void flushDirtyPage() {
//		synchronized (mutex) {
//
//			if (deleteIntList.size() != 0) {
//				numRecord -= deleteIntList.size();
//				deleteCheckpointPrepare();
//			}
//
//			TreeSet<Integer> dirtyPage = new TreeSet<Integer>(); // flush할 page id
//
//			int colCnt = columns.size(); // 컬럼의 갯수
//
//			int len = table.getLen();
//			int pageSwap = PAGE_SIZE / len;
//
//			if (dirtyPageIdList.size() == 0)
//				return;
//
//			for (int ridIdx : dirtyPageIdList) {
//				Tuple temp = tuples.get(ridIdx);
//
//				int rid = temp.rid.tuplePos;
//				// System.out.println("checkpointing " + rid);
//
//				int pageid = (int) (rid / pageSwap) + 1;
//				int pageoffset = rid % pageSwap * len;
//
//				dirtyPage.add(pageid);
//
//				for (int j = 0; j < colCnt; j++) { // 컬럼 별로 읽기
//					TypeBase typeTemp = attrTypes.get(j);
//					TypeEnum enumTemp = typeTemp.type;
//
//					if (enumTemp == TypeEnum.INT) {
//
//						Convert.setIntValue(Integer.parseInt(temp.getValues().get(j).getValue().toString()), pageoffset,
//								tbsData[pageid]);
//					} else if (enumTemp == TypeEnum.CHAR) {
//						Convert.setStringValue(temp.getValues().get(j).getValue().toString(), pageoffset,
//								tbsData[pageid], typeTemp.getMaxSize());
//					}
//					pageoffset += typeTemp.getMaxSize();
//				}
//			}
//			dirtyPageIdList.clear(); // 더티페이지 리셋
//			Iterator iter = dirtyPage.iterator(); // Iterator 사용
//
//			// header 기록 -> numrecord , byte length
//			Convert.setIntValue(numRecord + 1, 0, tbsData[0]);
//			Convert.setIntValue(table.getLen(), 4, tbsData[0]);
//
//			disk.write_page(new Page(0, tbsData[0]));
//			while (iter.hasNext()) { // 값이 있으면 true 없으면 false
//				int pid = (Integer) iter.next();
//
//				disk.write_page(new Page(pid, tbsData[pid]));
//			}
//			System.out.println("dirty page checkpoint end!");
//		}
//	}

	public void flushAll() {
		int pidFlush = 0;
		int offsetFlush = 0;

		int colCnt = columns.size(); // 컬럼의 갯수

		Convert.setIntValue(ridList.size(), offsetFlush, tbsData[pidFlush++]);
		offsetFlush += 4;

		for (RID ridTemp : ridList) {
			Tuple temp = tuples.get(ridTemp.tuplePos);

			for (int j = 0; j < colCnt; j++) { // 컬럼 별로 읽기
				TypeBase typeTemp = attrTypes.get(j);
				TypeEnum enumTemp = typeTemp.type;

				if (offsetFlush + typeTemp.getMaxSize() >= PAGE_SIZE) { // 만약 페이지의 크기를 넘어선다면
					offsetFlush = 0;
					pidFlush++;
				}

				if (enumTemp == TypeEnum.INT) {

					Convert.setIntValue(Integer.parseInt(temp.getValues().get(j).getValue().toString()), offsetFlush,
							tbsData[pidFlush]);
				} else if (enumTemp == TypeEnum.CHAR) {

					Convert.setStringValue(temp.getValues().get(j).getValue().toString(), offsetFlush,
							tbsData[pidFlush], typeTemp.getMaxSize());
				}
				offsetFlush += typeTemp.getMaxSize();
			}
		}

		disk.allWriteFile(tbsData, pidFlush);
		try {
			disk.closeDB();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 칼럼의 인덱스를 반환.
	 * 
	 * @param whereCondition where 절에 있던 칼럼
	 * @return
	 */
	public int getIdx(String whereCondition) {
		return tableInfo.get(whereCondition);
	}

	/**************** 데이터 조작 (DML) *********************/

	/**
	 * insert 시 tuple 추가
	 * 
	 * @param values value 들
	 * @return
	 */
	public RID addTuple(List<TypeBase> values) {
		if (lastPos + table.getLen() >= PAGE_SIZE) {
			pageEndNum++;
			lastPos = 0;
		}

		RID temp = new RID(fid, pageEndNum, lastPos, tuples.size());
		lastPos += table.getLen(); // pos 증가
		this.numRecord++; // row 수 증가

		dirtyPageIdList.add(numRecord);
		tuples.add(new Tuple(values, temp));
		return temp;
	}

	/**
	 * out of update시, tuple 추가
	 * 
	 * @param t 바뀌는 tuple
	 * @return
	 */
	public int addTuple(Tuple t) {
		// 튜플 추가
		this.tuples.add(t);
		btreeInfo.get("C1").insert(Integer.parseInt(t.getValues().get(this.index).getValue().toString()), t);
		return tuples.size() - 1;
	}

	public List<Tuple> getTuples() {
		return this.tuples;
	}

	public int getTupleSize() {
		return this.tuples.size();
	}

	/******** select ***********/

	public List<Tuple> getByRID(List<Tuple> temp, int sessionId, int viewSCN) {
		List<Tuple> result = new ArrayList<Tuple>();
		LinkedList<Tuple> result1 = new LinkedList<Tuple>();
		TransactionManager.getInstance().setExplain(sessionId, "by row id\n");

		if (temp == null || temp.size() == 0)
			return null;

		for (Tuple t : temp) {
			if (t.getSessionID() == sessionId && t.getNext() != null) { // 본인 세션이면 읽기
				result1.add(t.getNext());
			} else if (t.getSessionID() == sessionId && t.getNext() == null) { // delete

			} else if ((t.getScn() <= viewSCN) && t.infomask) { // 읽을 수 있는 시기
				// 그대로 읽기
				result1.add(t);
			}
//			else if(previous != null && previous.rid.tuplePos > t.rid.tuplePos){ // 커밋이 된 dirty page 이고 order by 가 아니라면 rid																					// 맞추어 정렬
//				int i = 0;
//				int pos = t.rid.tuplePos;
//				System.out.println(previous.rid.tuplePos +" "+t.rid.tuplePos);
//				for (i = 0; i < result.size(); i++) {
//					if (pos < result.get(i).rid.tuplePos )
//						break;
//				}
//				result1.add(i, t);
//			} 
		}
		Collections.sort(result1, new Comparator<Tuple>() {
			@Override
			public int compare(Tuple o1, Tuple o2) {
				if (o1.rid.tuplePos > o2.rid.tuplePos)
					return +1;
				else
					return -1;
			}
		});

		for (Tuple t : result1) {
			result.add(t);
		}
		return result;
	}

	public List<Tuple> visibleRID(List<Tuple> temp, int sessionId, int viewSCN) {
		List<Tuple> result = new ArrayList<Tuple>();
		LinkedList<Tuple> result1 = new LinkedList<Tuple>();
		TransactionManager.getInstance().setExplain(sessionId, "by row id\n");

		if (temp == null || temp.size() == 0)
			return null;

		for (Tuple t : temp) {
			if (t.getSessionID() == sessionId && t.getNext() != null) { // 본인 세션이면 읽기
				result1.add(t.getNext());
			} else if (t.getSessionID() == sessionId && t.getNext() == null) { // delete

			} else if ((t.getScn() <= viewSCN) && t.infomask) { // 읽을 수 있는 시기
				// 그대로 읽기
				result1.add(t);
			}
//			else if(previous != null && previous.rid.tuplePos > t.rid.tuplePos){ // 커밋이 된 dirty page 이고 order by 가 아니라면 rid																					// 맞추어 정렬
//				int i = 0;
//				int pos = t.rid.tuplePos;
//				System.out.println(previous.rid.tuplePos +" "+t.rid.tuplePos);
//				for (i = 0; i < result.size(); i++) {
//					if (pos < result.get(i).rid.tuplePos )
//						break;
//				}
//				result1.add(i, t);
//			} 
		}

		for (Tuple t : result1) {
			result.add(t);
		}
		return result;
	}

	// 인덱스가 아닐 때, full scan
	public List<Tuple> doFullScan(String whereCondition, Object ob) {
		List<Tuple> result = new ArrayList<Tuple>();
		int idx = getIdx(whereCondition);

		for (Tuple t : tuples) {
			if (Integer.parseInt(t.getValues().get(idx).getValue().toString()) == (Integer) ob)
				result.add(t);
		}
		return result;
	}

	// range 검색 full scan
	public List<Tuple> doFullScanRange(String whereCondition, Object start, Object end, RangePolicy policy) {
		List<Tuple> result = new ArrayList<Tuple>();
		int idx = getIdx(whereCondition);

		for (Tuple t : tuples) {
			int comp = Integer.parseInt(t.getValues().get(idx).getValue().toString());
			int cmp1 = Integer.compare(comp, (Integer) start);
			int cmp2 = Integer.compare(comp, (Integer) end);
			if (((policy == RangePolicy.EXCLUSIVE && cmp1 > 0) || (policy == RangePolicy.INCLUSIVE && cmp1 >= 0))
					&& ((policy == RangePolicy.EXCLUSIVE && cmp2 < 0)
							|| (policy == RangePolicy.INCLUSIVE && cmp2 <= 0))) {
				result.add(t);
			}
		}
		return result;
	}

	public List<Tuple> doWhere(String whereCondition, Object ob, int sessionId, int viewSCN) {

		// 컬럼이 인덱스인지 확인
		if (!btreeInfo.containsKey(whereCondition)) {
			TransactionManager.getInstance().setExplain(sessionId, "full table scan\n");
			return getByRID(doFullScan(whereCondition, ob), sessionId, viewSCN);
		} else {
			// 검색 값으로 찾기
			// System.out.println("index scan");
			TransactionManager.getInstance().setExplain(sessionId, "index scan\n");
			List<Tuple> bplusstree = btreeInfo.get(whereCondition).search((Integer) ob);
			return visibleRID(bplusstree, sessionId, viewSCN);
		}
	}

	public List<Tuple> doWhereRange(String whereCondition, Object start, Object end, RangePolicy policy, int sessionId,
			int viewSCN) {
		// 컬럼이 인덱스인지 확인
		if (!btreeInfo.containsKey(whereCondition)) {
			TransactionManager.getInstance().setExplain(sessionId, "full table scan\n");
			return getByRID(doFullScanRange(whereCondition, start, end, policy), sessionId, viewSCN);
		} else {
			// 찾기
			TransactionManager.getInstance().setExplain(sessionId, "index range scan\n");
			// System.out.println("index range scan");
			List<Tuple> bplusstree = btreeInfo.get(whereCondition).searchRange((Integer) start, policy, (Integer) end,
					policy);

			return visibleRID(bplusstree, sessionId, viewSCN);
		}
	}

	/**
	 * order by sort - 인덱스 컬럼일 경우
	 * 
	 * @param colName
	 * @return
	 */
	public List<Tuple> getAllSortedValues(String colName) {
		// 컬럼이 인덱스인지 확인
		if (!btreeInfo.containsKey(colName)) {
			return null;
		}

		List<Tuple> sortedValues = btreeInfo.get(colName).getAllValues();

//		for(Tuple t : sortedValues)
//			System.out.println(t.getValues().get(0).getValue().toString());

		return sortedValues;
	}

	/**
	 * 
	 * update 시에 지정한 offset 에 있어야할 실제 data 세팅
	 * 
	 * @param offset page 내에서의 slot id
	 */
	public void setSlot(int offset) {
		dirtyPageIdList.add(offset);
		// ridList.set(offset, rid);
	}

	/**
	 * 
	 * delete tuples
	 * 
	 * 1.인덱스 트리에서 못읽게 false 처리 2. dirty page list 에 tuple 뒤에 있는 tuple 들에서 rid 큰거 전부
	 * 앞으로 당기기
	 * 
	 * @return
	 */
	public int delete(List<Tuple> deleteTuples, int sessionId) {
		LogManager lm = LogManager.getInstance();

		int result = 0;

		if (deleteTuples == null)
			return 0;
		for (int i = 0; i < deleteTuples.size(); i++) {
			Tuple t = deleteTuples.get(i);
			result++;
			t.setSessionID(sessionId);

			lm.addDeleteTuple(t);
		}
		return result;
	}

	public void deleteCheckpointPrepare() {
		int colCnt = columns.size(); // 컬럼의 갯수

		int len = table.getLen();
		int pageSwap = PAGE_SIZE / len;
		if(deleteIntList.size() == 0) return;
		
		System.out.println(deleteIntList.size());
		for (int ridIdx : deleteIntList) {
			Tuple temp = tuples.get(ridIdx);

			int pageid = temp.rid.pageID;
			int rid = temp.rid.offset;

			for (int i = 0; i < len; i++) {
				tbsData[pageid][rid+i] = 0;
			}
		}
		// Iterator 사용예제
		Iterator<Integer> it = deleteIntList.iterator();

		int cursor = -1;
		if (it.hasNext())
			cursor = it.next() + 1; // 맨 처음 삭제된 rid +1 부터 조정해야됨.
		else
			return;

		int init = cursor - 1;

		
		boolean in = true;
		int pos = 1; // 앞에서 지워진 횟수
		while (in) {
			int post = Integer.MAX_VALUE;
			if (it.hasNext()) {
				post = it.next();
				
				pos++;
			}
			//System.out.println("post value "+post);
			
			for (; (cursor < post && cursor < this.tuples.size()); cursor++) {
				Tuple temp = tuples.get(cursor);

				//System.out.println("prior"+temp.rid.tuplePos + "pos"+pos);
				// System.out.println(temp.rid.tuplePos);
				if ((temp.rid.tuplePos - pos) >= 0 && temp.infomask) {// 새로 업데이트가 된 것이 아니라면
					
					temp.rid.tuplePos -= pos;
					
					//System.out.println("after"+temp.rid.tuplePos);
					int rid = temp.rid.tuplePos;
					temp.rid.pageID = (int) (temp.rid.tuplePos / pageSwap) + 1;
					temp.rid.offset = rid % pageSwap * len;
					
					this.dirtyPageIdList.add(cursor);
				} else if (!temp.infomask && temp.getNext() != null) {
					Tuple t = temp.getNext();
					if (t.rid.tuplePos - pos >= 0) {
						System.out.println("prior"+temp.rid.tuplePos);
						t.rid.tuplePos -= pos;
						
						//System.out.println("after"+temp.rid.tuplePos);
						int rid = temp.rid.tuplePos;
						temp.rid.pageID = (int) (temp.rid.tuplePos / pageSwap) + 1;
						temp.rid.offset = rid % pageSwap * len;
						
						this.dirtyPageIdList.add(t.rid.curOffset);
					}
				}
			}
			cursor++;
			if (!it.hasNext()) {
				for (; (cursor < this.tuples.size()); cursor++) {
					Tuple temp = tuples.get(cursor);

					//System.out.println(temp.rid.tuplePos);
					if ((temp.rid.tuplePos - pos) >= 0 && temp.infomask) {// 새로 업데이트가 된 것이 아니라면
						temp.rid.tuplePos -= pos;
						
						int rid = temp.rid.tuplePos;
						temp.rid.pageID = (int) (rid / pageSwap) + 1;
						temp.rid.offset = rid % pageSwap * len;
						
						this.dirtyPageIdList.add(cursor);
					} else if (!temp.infomask && temp.getNext() != null) {
						Tuple t = temp.getNext();
						if (t.rid.tuplePos - pos >= 0) {
							t.rid.tuplePos -= pos;
							
							int rid = temp.rid.tuplePos;
							temp.rid.pageID = (int) (rid / pageSwap) + 1;
							temp.rid.offset = rid % pageSwap * len;
							
							this.dirtyPageIdList.add(t.rid.curOffset);
						}
					}
				}

				in = false;
			}
		}

		//System.out.println(pos);
		deleteIntList.clear();
	}
}
