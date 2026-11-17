package edu.pss.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Clase de arranque de la aplicación (la crea Spring automáticamente al hacer el proyecto).
 *
 * Al ejecutarla, Spring Boot arranca, busca las clases marcadas con @Configuration
 * (en esta práctica, ConfigBatch1), crea todas sus piezas y lanza el trabajo (JOB) él solo.
 */
@SpringBootApplication
public class PracticalocalidadesApplication {

	// Punto de entrada: es lo primero que se ejecuta al darle a Run
	public static void main(String[] args) {
		SpringApplication.run(PracticalocalidadesApplication.class, args);
	}

}
