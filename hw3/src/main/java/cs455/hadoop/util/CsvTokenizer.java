package cs455.hadoop.util;

import java.util.ArrayList;

public class CsvTokenizer {

    private final String line;
    private final ArrayList<TokenDesc> tokenDescs = new ArrayList<>(32);

    public CsvTokenizer(String line){
        this.line = line;
        generateIndices();
    }

    public String getTokAt(int idx) {
        TokenDesc desc = tokenDescs.get(idx);
        return line.substring(desc.start, desc.end);
    }

    public double getTokAsDouble(int idx) throws Exception {

        String tok = getTokAt(idx);

        if(tok.equals(""))
            return 0;

        try {
            double ret = Double.parseDouble(getTokAt(idx));

            return ret;
        } catch (Exception e) {
            throw new Exception("error getting double at idx:"+idx);
        }
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
