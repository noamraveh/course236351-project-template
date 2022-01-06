package com.example.api.controller

import com.example.api.repository.model.Employee
import com.example.api.service.EmployeeService
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestBody

/**
 * Controller for REST API endpoints
 */
@RestController
class EmployeeController(private val employeeService: EmployeeService) {

//    List the entire ledger history since the Genesis UTxO ordered by the transaction timestamps. It should also support a limit on the number of transactions being returned.
    @GetMapping("/tx/{n}")
    fun getLedgerHistory(@PathVariable("n") n: Int): List<Tx> =
        employeeService.getTxs()

//    Send an amount of coins to the given address. The method should select any of the available UTxOs in- order to satisfy the request amount.

    //    List the entire transaction history for the given ad- dress ordered by the transaction timestamps. It should also support a limit on the number of transac- tions being returned.
    @GetMapping("/tx/{addr}/{n}")
    fun getAddrHistory(@PathVariable("addr") addr: Int, @PathVariable("n") n: Int): List<Tx> =
        employeeService.getTxs(addr)

    //    List all unspent transaction outputs for the given ad- dress
    @GetMapping("/utxo/{addr}")
    fun getAddrHistory(@PathVariable("addr") addr: Int, @PathVariable("n") n: Int): List<UTxO> =
        employeeService.getTxs(addr)

//    Submit a transaction / an atomic transaction list  => get the created tx id(s)
    @PostMapping("/tx")
    fun createEmployee(@RequestBody txList : List<Tx>): List<Int> =
        employeeService.submitTx(txList)

    @PostMapping("/tr/{addr}/{amount}")
    fun createEmployee(@PathVariable("addr") addr: Int, @PathVariable("amount") amount: Int): Int =
        employeeService.submitTr(addr,amount)

}