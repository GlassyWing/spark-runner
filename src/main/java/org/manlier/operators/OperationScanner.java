package org.manlier.operators;

import org.manlier.utils.ClassScanner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 算子扫描器，扫描指定包下的所有算子
 * <p>
 * Create by manlier 2018/8/13 10:08
 */
public class OperationScanner {

    private ClassScanner classScanner;

    public OperationScanner(String basePackage) {
        this.classScanner = new ClassScanner(basePackage);
    }

    @SuppressWarnings("unchecked")
    public Map<OpType, Map<String, Class<Operation>>> getAllOperations() {
        List<String> classNameList;
        try {
            classNameList = this.classScanner.getFullyQualifiedClassNameList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Map<OpType, Map<String, Class<Operation>>> operations = new HashMap<>();

        classNameList.parallelStream().map(s -> {
            try {
                return Class.forName(s);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        })
                .filter(Operation.class::isAssignableFrom)
                .filter(clazz -> clazz.getAnnotation(Op.class) != null)
                .forEach(clazz -> {
                    Op op = clazz.getAnnotation(Op.class);
                    operations.computeIfAbsent(op.type(), k -> new HashMap<>());
                    operations.get(op.type()).put(op.name(), (Class<Operation>) clazz);
                });
        return operations;
    }

}
