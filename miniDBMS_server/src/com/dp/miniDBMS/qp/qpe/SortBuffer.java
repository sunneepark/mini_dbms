package com.dp.miniDBMS.qp.qpe;

import java.util.List;

import com.dp.miniDBMS.cm.TransactionManager;
import com.dp.miniDBMS.sm.smo.Tuple;

/**
  *
  * @날짜 : Aug 23, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : temporary 하게 정렬할 때 필요한 임시 버퍼 
  *
 */
public class SortBuffer {
	public static int idx = 0;
	public static void quickSort(List<Tuple> arr, int sessionId, int searchIdx) {
		TransactionManager.getInstance().setExplain(sessionId, "using temporary\n");
		idx = searchIdx;
		sort(arr, 0, arr.size() - 1);
	}

	private static void sort(List<Tuple> arr, int low, int high) {
		if (low >= high)
			return;
		int mid = partition(arr, low, high);
		sort(arr, low, mid - 1);
		sort(arr, mid, high);
	}

	private static int partition(List<Tuple> arr, int low, int high) {
		int pivot = turnInt(arr.get((low + high) / 2));
		while (low <= high) {
			while (turnInt(arr.get(low)) < pivot)
				low++;
			while (turnInt(arr.get(high)) > pivot)
				high--;
			if (low <= high) {
				swap(arr, low, high);
				low++;
				high--;
			}
		}
		return low;
	}

	private static void swap(List<Tuple> arr, int i, int j) {
		Tuple tmp = arr.get(i);
		arr.set(i, arr.get(j));
		arr.set(j, tmp);
	}
	private static int turnInt(Tuple t) {
		return Integer.parseInt(t
				.getValues().get(idx).getValue().toString());
		
	}
}
