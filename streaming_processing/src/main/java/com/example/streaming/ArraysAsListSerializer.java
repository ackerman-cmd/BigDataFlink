package com.example.streaming;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.ArrayList;
import java.util.List;

public class ArraysAsListSerializer extends Serializer<List<?>> {


    @Override
    public void write(Kryo kryo, Output output, List<?> object) {
        output.writeInt(object.size(), true); // Записываем размер списка
        for (Object item : object) {
            kryo.writeClassAndObject(output, item); // Записываем каждый элемент с информацией о классе
        }
    }

    @Override
    public List<?> read(Kryo kryo, Input input, Class<List<?>> aClass) {
        int size = input.readInt();
        List<Object> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(kryo.readClassAndObject(input));
        }
        return list;
    }

}