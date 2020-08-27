package com.dp.miniDBMS.sm.smm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.dp.miniDBMS.sm.smo.Tuple;

/**
 * 
 *
 * @날짜 : Aug 8, 2020
 * @작성자 : 박선희
 * @클래스설명 : B+ tree
 * 
 * oss 맨밑에 기재. search, update, delete 일부 변경 (value 값과 value 출력 및 range 검색 변경)
 * 
 */
public class BPlusTree<K extends Comparable<? super K>> {

	public static enum RangePolicy {
		EXCLUSIVE, INCLUSIVE
	}

	/**
	 * The branching factor used when none specified in constructor.
	 * 
	 * 한 노드에 몇 개의 key를 담을 건지 -> 브랜치가 되는 노드 갯수 조절.
	 */
	private static final int DEFAULT_BRANCHING_FACTOR = 128;

	/**
	 * The branching factor for the B+ tree, that measures the capacity of nodes
	 * (i.e., the number of children nodes) for internal nodes in the tree.
	 * 
	 */
	private int branchingFactor;

	/**
	 * The root node of the B+ tree.
	 * 
	 * 비플러스트리의 루트 노드
	 */
	private Node root;

	public BPlusTree() {
		this(DEFAULT_BRANCHING_FACTOR);
	}

	/**
	 * 한 개의 노드에 몇 개의 key 가 들어갈 수 있는지.
	 * @param branchingFactor 브랜치 되는 갯수
	 */
	public BPlusTree(int branchingFactor) { 
		if (branchingFactor <= 2)
			throw new IllegalArgumentException("Illegal branching factor: " + branchingFactor);
		this.branchingFactor = branchingFactor;
		root = new LeafNode();
	}

	/**
	 * Returns the value to which the specified key is associated, or {@code null}
	 * if this tree contains no association for the key.
	 *
	 * <p>
	 * A return value of {@code null} does not <i>necessarily</i> indicate that the
	 * tree contains no association for the key; it's also possible that the tree
	 * explicitly associates the key to {@code null}.
	 * 
	 * key에 알맞은 value 값 반환. 루트부터 검색하면서 확인 
	 * 
	 * @param key the key whose associated value is to be returned
	 * 
	 * @return the value to which the specified key is associated, or {@code null}
	 *         if this tree contains no association for the key
	 */
	public List<Tuple> search(K key) {
		return root.getValue(key);
	}

	/**
	 * Returns the values associated with the keys specified by the range:
	 * {@code key1} and {@code key2}.
	 * 
	 * key의 range 에 맞는 value 반환. 
	 * 
	 * @param key1    the start key of the range
	 * @param policy1 the range policy, {@link RangePolicy#EXCLUSIVE} or
	 *                {@link RangePolicy#INCLUSIVE}
	 * @param key2    the end end of the range
	 * @param policy2 the range policy, {@link RangePolicy#EXCLUSIVE} or
	 *                {@link RangePolicy#INCLUSIVE}
	 * @return the values associated with the keys specified by the range:
	 *         {@code key1} and {@code key2}
	 */
	public List<Tuple> searchRange(K key1, RangePolicy policy1, K key2, RangePolicy policy2) {
		return root.getRange(key1, policy1, key2, policy2);
	}

	/**
	 * 
	 * 
	 * 키와 밸류를 삽입. 만약 key old value 일 경우 value의 list에 value만 추가.
	 * 
	 * @param key   the key with which the specified value is to be associated
	 * @param value the value to be associated with the specified key
	 */
	public void insert(K key, Tuple value) {
		root.insertValue(key, value);
	}

	/**
	 * Removes the association for the specified key from this tree if present.
	 * 
	 * key 삭제
	 * 
	 * @param key the key whose association is to be removed from the tree
	 */
	public void delete(K key) {
		root.deleteValue(key);
	}
	
	/**
	 * 모든 value들 정렬된 순서대로 가져오기
	 * 
	 * @return 트리에 있는 모든 value
	 */
	public List<Tuple> getAllValues() {
		List<Tuple> result = new LinkedList<Tuple>(); // 전체 튜플 담는 곳

		Queue<List<Node>> queue = new LinkedList<List<Node>>();

		queue.add(Arrays.asList(root)); // 루트 노드 부터 타고 들어가기

		while (!queue.isEmpty()) {
			Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
			while (!queue.isEmpty()) {
				List<Node> nodes = queue.remove();
				Iterator<Node> it = nodes.iterator();
				while (it.hasNext()) { // 다음 노드가 있으면
					Node node = it.next();

					if(node instanceof BPlusTree.LeafNode) {
						List<List<Tuple>> temp = node.getValues();
						for(List<Tuple> t : temp)
							result.addAll(t);
					}
						
					if (node instanceof BPlusTree.InternalNode)
						nextQueue.add(((InternalNode) node).children);
				}
			}
			queue = nextQueue;
		}
		return result;
	}

	public String toString() {
		Queue<List<Node>> queue = new LinkedList<List<Node>>();
		queue.add(Arrays.asList(root));
		StringBuilder sb = new StringBuilder();
		while (!queue.isEmpty()) {
			Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
			while (!queue.isEmpty()) {
				List<Node> nodes = queue.remove();
				sb.append('{');
				Iterator<Node> it = nodes.iterator();
				while (it.hasNext()) {
					Node node = it.next();

					sb.append(node.toString()); // node의 key를 string으로 바꿈.
					if (it.hasNext())
						sb.append(", ");
					if (node instanceof BPlusTree.InternalNode)
						nextQueue.add(((InternalNode) node).children);
				}
				sb.append('}');
				if (!queue.isEmpty())
					sb.append(", ");
				else
					sb.append('\n');
			}
			queue = nextQueue;
		}

		return sb.toString();
	}
	
	/**
	 * 
	  *
	  * @날짜 : Aug 8, 2020 
	  * @작성자 : 박선희 
	  * @클래스설명 : 트리 노드의 추상 클래스 
	  *
	 */
	private abstract class Node {
		List<K> keys; // 현재 노드의 키 값.

		int keyNumber() {
			return keys.size();
		}

		abstract List<Tuple> getValue(K key);

		abstract List<List<Tuple>> getValues();

		abstract void deleteValue(K key);

		abstract void insertValue(K key, Tuple value);

		abstract K getFirstLeafKey();

		abstract List<Tuple> getRange(K key1, RangePolicy policy1, K key2, RangePolicy policy2);

		abstract void merge(Node sibling);

		abstract Node split();

		abstract boolean isOverflow();

		abstract boolean isUnderflow();

		public String toString() {
			return keys.toString();
		}
	}

	/**
	 * 
	  *
	  * @날짜 : Aug 8, 2020 
	  * @작성자 : 박선희 
	  * @클래스설명 : internal node : leaf node 가 아닌 key의 binary search 만을 위한 노드.
	  *
	 */
	private class InternalNode extends Node { 
		List<Node> children; // 해당 노드에 있는 

		InternalNode() {
			this.keys = new ArrayList<K>();
			this.children = new ArrayList<Node>();
		}

		@Override
		List<Tuple> getValue(K key) {
			return getChild(key).getValue(key);
		}

		@Override
		void deleteValue(K key) {
			Node child = getChild(key);
			child.deleteValue(key);
			
			if (child.isUnderflow()) {
				Node childLeftSibling = getChildLeftSibling(key);
				Node childRightSibling = getChildRightSibling(key);
				Node left = childLeftSibling != null ? childLeftSibling : child;
				Node right = childLeftSibling != null ? child : childRightSibling;
				left.merge(right);
				deleteChild(right.getFirstLeafKey());
				if (left.isOverflow()) {
					Node sibling = left.split();
					insertChild(sibling.getFirstLeafKey(), sibling);
				}
				if (root.keyNumber() == 0)
					root = left;
			}
		}

		@Override
		void insertValue(K key, Tuple value) { // value 삽입
			Node child = getChild(key); // 노드로 부터 얻기.
			child.insertValue(key, value);

			if (child.isOverflow()) { // 삽입해야 할 노드가 overflow 시에
				Node sibling = child.split(); // 노드 나눈 후
				insertChild(sibling.getFirstLeafKey(), sibling); // 나눈 node에 value 삽입 
			}
			if (root.isOverflow()) { // root가 overflow 시에
				Node sibling = split(); // root 나눈 후,
				
				InternalNode newRoot = new InternalNode(); //value 담을 말단 노드 추가 
				newRoot.keys.add(sibling.getFirstLeafKey()); //첫번째 자식에 key 추가
				newRoot.children.add(this); //현재와 
				newRoot.children.add(sibling); //자식들 전부 연결
				root = newRoot; //root 로 올림 
			}
		}

		@Override
		K getFirstLeafKey() {
			return children.get(0).getFirstLeafKey();
		}

		@Override
		List<Tuple> getRange(K key1, RangePolicy policy1, K key2, RangePolicy policy2) {
			return getChild(key1).getRange(key1, policy1, key2, policy2);
		}

		@Override
		void merge(Node sibling) {
			@SuppressWarnings("unchecked")
			InternalNode node = (InternalNode) sibling;
			keys.add(node.getFirstLeafKey());
			keys.addAll(node.keys);
			children.addAll(node.children);

		}

		@Override
		Node split() {
			int from = keyNumber() / 2 + 1, to = keyNumber(); //반으로 나눔.
			InternalNode sibling = new InternalNode(); //새로운 노드 생성. 
			sibling.keys.addAll(keys.subList(from, to)); //새로운 노드에 반으로 나눈 후반을 붙이고 
			sibling.children.addAll(children.subList(from, to + 1)); //새로운 노드의 자식을 원래 노드의 자식으로 붙인다. 

			keys.subList(from - 1, to).clear(); //새로운 노드에 추가한 부분과 위로 올라간 부분은 제외. 
			children.subList(from, to + 1).clear(); //제외한 곳에 알맞는 children 값도 제외.

			return sibling;
		}

		/**
		 * 한 노드의 키 갯수가 많을 때 (같을 때 아님)
		 */
		@Override
		boolean isOverflow() {
			return children.size() > branchingFactor;
		}

		@Override
		boolean isUnderflow() {
			return children.size() < (branchingFactor + 1) / 2;
		}

		Node getChild(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			return children.get(childIndex);
		}

		void deleteChild(K key) {
			int loc = Collections.binarySearch(keys, key);
			if (loc >= 0) {
				keys.remove(loc);
				children.remove(loc + 1); //객체 삭제 
			}
		}

		/**
		 * child insert 할 때,
		 * 
		 * 같은 곳이 있으면, child 배정 아니면, key 추가하고, children 배열 늘리기
		 * 
		 * @param key
		 * @param child
		 */
		void insertChild(K key, Node child) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (loc >= 0) { // 이미 같은 값이 있을때 
				children.set(childIndex, child);
			} else { 
				keys.add(childIndex, key);
				children.add(childIndex + 1, child);
			}
		}

		Node getChildLeftSibling(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex > 0)
				return children.get(childIndex - 1);

			return null;
		}

		Node getChildRightSibling(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex < keyNumber())
				return children.get(childIndex + 1);

			return null;
		}

		@Override
		List<List<Tuple>> getValues() { // 맨 처음 키
			return children.get(0).getValues();
		}
	}

	/**
	 * 
	 *
	 * @날짜 : Aug 9, 2020
	 * @작성자 : 박선희
	 * @클래스설명 : leaf level node > 말단 인덱스 엔트리의 linked list
	 * 
	 *        ** 최종적인 value 값을 지니고 있는 노드 **
	 *
	 */
	private class LeafNode extends Node {
		List<List<Tuple>> values;
		LeafNode next;

		LeafNode() {
			keys = new ArrayList<K>();
			values = new ArrayList<List<Tuple>>();
		}

		@Override
		List<Tuple> getValue(K key) {
			int loc = Collections.binarySearch(keys, key);
			return loc >= 0 ? values.get(loc) : null; // loc 찾았으면.
		}

		@Override
		void deleteValue(K key) {
			int loc = Collections.binarySearch(keys, key);
			
			if (loc >= 0) {
				keys.remove(loc);
				values.remove(loc);
			} 
		}

		/**
		 * 같은 key 값일 때, value 추가
		 */
		@Override
		void insertValue(K key, Tuple value) {

			int loc = Collections.binarySearch(keys, key); // binary search 로 key 의 위치 찾기. -> 찾지 못했으면 -1 , 존재하면 0보다 큰값.
			int valueIndex = loc >= 0 ? loc : -loc - 1; // 값이 존재하면 out. 존재하지 않으면 삽입
			
			if (loc >= 0) { // 값이 존재하면 value change가 아닌 add
				values.get(valueIndex).add(value);
			} else { //값이 없더라면 
				keys.add(valueIndex, key);
				values.add(valueIndex, new ArrayList<Tuple>()); // 새로운 노드 추가.
				values.get(valueIndex).add(value);
			}
			if (root.isOverflow()) {
				Node sibling = split();
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this);
				newRoot.children.add(sibling);
				root = newRoot;
			}
		}

		@Override
		K getFirstLeafKey() {
			return keys.get(0);
		}

		@Override
		List<Tuple> getRange(K key1, RangePolicy policy1, K key2, RangePolicy policy2) {

			List<Tuple> result = new ArrayList<Tuple>();
			LeafNode node = this;
			while (node != null) {
				Iterator<K> kIt = node.keys.iterator();
				Iterator<List<Tuple>> vIt = node.values.iterator();
				while (kIt.hasNext()) {
					K key = kIt.next();
					List<Tuple> value = vIt.next();
					
					int cmp1 = key.compareTo(key1);
					int cmp2 = key.compareTo(key2);
					if (((policy1 == RangePolicy.EXCLUSIVE && cmp1 > 0)
							|| (policy1 == RangePolicy.INCLUSIVE && cmp1 >= 0))
							&& ((policy2 == RangePolicy.EXCLUSIVE && cmp2 < 0)
									|| (policy2 == RangePolicy.INCLUSIVE && cmp2 <= 0))) {
						result.addAll(value);
					}
					else if ((policy2 == RangePolicy.EXCLUSIVE && cmp2 >= 0)
							|| (policy2 == RangePolicy.INCLUSIVE && cmp2 > 0)) {
						return result;
					}
				}
				node = node.next;
			}
			return result;
		}

		@Override
		void merge(Node sibling) {
			@SuppressWarnings("unchecked")
			LeafNode node = (LeafNode) sibling;
			keys.addAll(node.keys);
			values.addAll(node.values);
			next = node.next;
		}

		@Override
		Node split() { //새로운 leaf node 를 만들어서 나눈 후, next로 리프노드간 포인터 연결.
			LeafNode sibling = new LeafNode();
			int from = (keyNumber() + 1) / 2, to = keyNumber();
			sibling.keys.addAll(keys.subList(from, to));
			sibling.values.addAll(values.subList(from, to));

			keys.subList(from, to).clear();
			values.subList(from, to).clear();

			sibling.next = next; //원래 있던 next 리프노드를 추가한 리프노드에 연결.
			next = sibling; // 원래 있던 노드의 next는 새로 추가한 리프노드.
			return sibling;
		}

		@Override
		boolean isOverflow() { 
			return values.size() > branchingFactor - 1;
		}

		@Override
		boolean isUnderflow() {
			return values.size() < branchingFactor / 2;
		}

		@Override
		List<List<Tuple>> getValues() {
			return values;
		}
	}
}

/**
 * 
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Fang Jiaguo
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 * 
 */
