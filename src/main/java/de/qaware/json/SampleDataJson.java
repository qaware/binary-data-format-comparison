package de.qaware.json;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import javax.json.bind.annotation.JsonbCreator;

@Value
@Builder
@AllArgsConstructor(onConstructor_ = @JsonbCreator)
public class SampleDataJson {
    int int1;
    int int2;
    int int3;
    int int4;
    int int5;
    String string1;
    String string2;
    String string3;
    String string4;
    String string5;
}
