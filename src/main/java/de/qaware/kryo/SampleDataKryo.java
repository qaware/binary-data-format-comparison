package de.qaware.kryo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SampleDataKryo {
    private int int1;
    private int int2;
    private int int3;
    private int int4;
    private int int5;
    private String string1;
    private String string2;
    private String string3;
    private String string4;
    private String string5;
}
