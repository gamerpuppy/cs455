package cs455.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;

public class CsvTokenizer {

    private final String line;
    private final ArrayList<TokenDesc> tokenDescs = new ArrayList<>(32);

    public CsvTokenizer(String line){
        this.line = line;
        generateIndices();
    }

    public String getTokAt(int idx) throws IOException {
        if(idx >= tokenDescs.size())
            throw new IOException("error getting idx "+idx+":"+line);

        TokenDesc desc = tokenDescs.get(idx);
        return line.substring(desc.start, desc.end);
    }

    private void generateIndices() {
        int start = 0;
        int parenCount = 0;

        for(int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            switch (c) {
                    case ',':
                        if(parenCount == 0) {
                            tokenDescs.add(new TokenDesc(start, i));
                            start = i+1;
                        }
                        break;
                    case ')':
                        parenCount--;
                        break;
                    case '(':
                        parenCount++;
                        break;
            }

        }

        tokenDescs.add(new TokenDesc(start, line.length()));
    }

    private static class TokenDesc {
        final int start;
        final int end;

        TokenDesc(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

}
