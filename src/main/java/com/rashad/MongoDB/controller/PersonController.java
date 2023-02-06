package com.rashad.MongoDB.controller;

import com.rashad.MongoDB.collection.Person;
import com.rashad.MongoDB.service.PersonService;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/person")
public class PersonController {

    private final PersonService personService;

    @PostMapping
    public String addPerson(@RequestBody Person person) {
        return personService.addPerson(person);
    }

    @GetMapping
    public List<Person> getPersonStartWith(@RequestParam String name) {
        return personService.getPersonStartWith(name);
    }

    @GetMapping("/age")
    public List<Person> getByPersonAge(@RequestParam Integer minAge,
                                       @RequestParam Integer maxAge) {
        return personService.getByPersonAge(minAge, maxAge);
    }

    @GetMapping("/search")
    public Page<Person> searchPerson(
            @RequestParam(required = false) String name,
            @RequestParam(required = false) Integer minAge,
            @RequestParam(required = false) Integer maxAge,
            @RequestParam(required = false) String city,
            @RequestParam(defaultValue = "0") Integer page,
            @RequestParam(defaultValue = "5") Integer size
    ) {
        Pageable pageable = PageRequest.of(page, size);
        return personService.search(name, minAge, maxAge, city, pageable);
    }

    @GetMapping("/oldest")
    public List<Document> getOldestPerson() {
        return personService.getOldestPersonByCity();
    }

    @GetMapping("/populationByCity")
    public List<Document> getPopulationByCity() {
        return personService.getPopulationByCity();
    }
}
