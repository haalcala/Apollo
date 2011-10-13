package org.apollo.orm;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TypeTest<T> {

	  /**
	   * Get the actual type arguments a child class has used to extend a generic base class.
	   *
	   * @param baseClass the base class
	   * @param childClass the child class
	   * @return a list of the raw classes for the actual type arguments.
	   */
	public static <T> List<Class<?>> getTypeArguments(Class<T> baseClass, Class<? extends T> childClass) {
		Map<Type, Type> resolvedTypes = new HashMap<Type, Type>();
		Type type = childClass;
		// start walking up the inheritance hierarchy until we hit baseClass
		while (! getClass(type).equals(baseClass)) {
			if (type instanceof Class) {
				// there is no useful information for us in raw types, so just keep going.
				type = ((Class) type).getGenericSuperclass();
			}
			else {
				ParameterizedType parameterizedType = (ParameterizedType) type;
				Class<?> rawType = (Class) parameterizedType.getRawType();

				Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
				TypeVariable<?>[] typeParameters = rawType.getTypeParameters();
				for (int i = 0; i < actualTypeArguments.length; i++) {
					resolvedTypes.put(typeParameters[i], actualTypeArguments[i]);
				}

				if (!rawType.equals(baseClass)) {
					type = rawType.getGenericSuperclass();
				}
			}
		}
		
		return null;
	}
	    
	/**
	 * Get the underlying class for a type, or null if the type is a variable type.
	 * @param type the type
	 * @return the underlying class
	 */
	public static Class<?> getClass(Type type) {
		if (type instanceof Class) {
			return (Class) type;
		}
		else if (type instanceof ParameterizedType) {
			return getClass(((ParameterizedType) type).getRawType());
		}
		else if (type instanceof GenericArrayType) {
			Type componentType = ((GenericArrayType) type).getGenericComponentType();
			Class<?> componentClass = getClass(componentType);
			if (componentClass != null ) {
				return Array.newInstance(componentClass, 0).getClass();
			}
			else {
				return null;
			}
		}
		else {
			return null;
		}
	}
	
	public List<T> nothing() {
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TypeTest<String> set = new TypeTest<String>();
		
		System.out.println("-------------");
		
		System.out.println("-- set.getClass().getTypeParameters()");
		for (TypeVariable<?> string : set.getClass().getTypeParameters()) {
			System.out.println(string.getGenericDeclaration());
			System.out.println(string.getClass().getSimpleName());
			System.out.println(string.getClass().getName());
			System.out.println(string.getClass().getCanonicalName());
			System.out.println(string.getClass().getComponentType());
			System.out.println(string.getClass().getEnclosingClass());
			
			for (TypeVariable<?> string2 : string.getClass().getTypeParameters()) {
				System.out.println("### " + string2);
			}
		}
		
		for (Method m : set.getClass().getMethods()) {
			//if (m.getName().equals("add")) {
				System.out.println("*************");
				System.out.println(m);
				
				System.out.println("-- Listng parameter types");
				for (Class<?> string : m.getParameterTypes()) {
					System.out.println(string);
				}
			
				System.out.println("-- Listng generic parameter types");
				for (Type type : m.getGenericParameterTypes()) {
					System.out.println("type: " + type.getClass());
				}
				
				System.out.println(m.getGenericReturnType());
			//}
		}
		
	}

}
