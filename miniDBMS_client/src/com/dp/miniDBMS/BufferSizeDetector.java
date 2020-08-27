package com.dp.miniDBMS;

import java.io.BufferedInputStream;
import java.io.InputStream;

public class BufferSizeDetector extends BufferedInputStream {
       public static void main(String[] args) {
           BufferSizeDetector bsd = new BufferSizeDetector(null);

           System.err.println(System.getProperty("java.version"));
           System.err.println(bsd.getBufferSize());
       }

       public BufferSizeDetector(InputStream in) {
           super(in);
       }

       public int getBufferSize() {
           return super.buf.length;
       }
}
