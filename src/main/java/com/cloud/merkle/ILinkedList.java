package com.cloud.merkle;

public class ILinkedList<R> {
    INode<R> head;
    INode<R> tail;
    int size = 0;

    public void toArray(R[] dest) {
        INode<R> cur = this.head;
        int i = 0;
        while (cur != null) {
            dest[i] = cur.value;
            cur = cur.next;
            i++;
        }
    }

    public void addFirst(R value) {
        addFirst(new INode<>(value));
    }

    private void addFirst(INode<R> element) {
        if (this.head == null) {
            this.head = element;
            this.tail = element;
        } else {
            element.next = this.head;
            this.head = element;
        }
        this.size++;
    }

    public void addLast(R value) {
        addLast(new INode<>(value));
    }

    private void addLast(INode<R> element) {
        if (this.tail == null) {
            this.tail = element;
            this.head = element;
        } else {
            this.tail.next = element;
            this.tail = element;
        }
        this.size++;
    }

    public void addAll(ILinkedList<R> others) {
        if (this.tail == null) {
            this.head = others.head;
        } else {
            this.tail.next = others.head;
        }
        this.tail = others.tail;
        this.size += others.size;
    }

    public static class INode<R> {
        R value;
        INode<R> next;

        public INode(R value) {
            this.value = value;
        }
    }
}
