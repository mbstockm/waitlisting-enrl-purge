import groovy.sql.Sql
import groovy.transform.Canonical
import groovy.transform.Field

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDateTime

/**
 * After WL registration status period run Banner SFPWAIT waitlist enrollment purge job for part of term.
 * Prior to purging the data store SFRWLNT notifications & remaining waitlist registration records for historical reporting purposes.
 * @author Michael Stockman
 */
@GrabConfig(systemClassLoader = true)
@Grab(group = 'com.oracle',module = 'ojdbc8', version = 'current')

def waitlistPurgeRecords = []
Sql.withInstance(getDbProps()) { Sql sql ->
    // Add small sleep timer to delay this job slightly
    sleep(30000)

    waitlistPurgeRecords = getWaitlistPurgeTerms(sql)
    waitlistPurgeRecords.each {
        saveWaitlistDataBeforePurge(sql,it.purgeTerm,it.ptrm)
        runSfpwait(it.purgeTerm,it.ptrm,it.rsts)
    }
}

/**
 * Execute sub process to run the SFPWAIT waitlist purge process job.
 * @author Michael Stockman
 * @param purgeTerm
 * @param ptrm
 * @param rsts
 */
void runSfpwait(def purgeTerm, def ptrm, def rsts) {
    if (System.properties.'os.name'.toString().toUpperCase().contains('WINDOWS')) {
        println 'Skipped executing shell commands while running in windows development environment.'
        return
    }

    def t = LocalDateTime.now().format('yyyyMMddHHmmss')
    Path parameters = parameterFile(purgeTerm,ptrm,rsts,t)
    Path out = Paths.get(System.properties.'user.home','/gurjobs/PROD/waitlisting/sfpwait','sfpwait_' + purgeTerm + '_' + ptrm + '_' + t + '.out')
    Path lis = Paths.get(System.properties.'user.home','/gurjobs/PROD','sfpwait.lis')
    Path nlis = Paths.get(System.properties.'user.home','/gurjobs/PROD/waitlisting/sfpwait','sfpwait_' + purgeTerm + '_' + ptrm + '_' + t + '.lis')

    def env = System.getenv().collect { k, v -> "$k=$v"}
    env.add('TNS_ADMIN=/u03/banjobs/proxy_setup')
    env.add('TWO_TASK=PROD')

    def proc = ['/bin/bash','-c','sfpwait [SAISUSR]/@JSUB_PROD < ' + parameters.toAbsolutePath().toString() + ' > ' + out.toAbsolutePath().toString() + ' 2>&1']
            .execute(env,new File(System.properties.'user.home' + '/gurjobs/PROD')).waitFor()

    if (Files.exists(lis)) {
        Files.copy(lis,nlis)
        Files.delete(lis)
    }
}

/**
 * Create parameter file for running SFPWAIT process.
 * If running the job via command line without JS# a specific format needs to be recreated with System.in parameter file. This can be seen in the following example.
 *
 *
 * RUN SEQUENCE NUMBER:
 *
 * THIS JOBSTREAM WILL PURGE A STUDENTS REGISTRATION
 * INFORMATION FOR WAITLISTED COURSES
 *
 * PURGE TERM:  202280
 *
 * PART OF TERM (%=ALL): D1
 *
 * PART OF TERM (%=ALL):
 *
 * STATUS: WL
 *
 * STATUS:
 *
 * AUDIT (A) OR UPDATE (U): A
 *
 * DELETE ALL EXPIRED NOTIFICATIONS?(Y/[N]):  N
 *
 * IF PURGE CRITERIA CORRECT, PRESS ENTER.  IF YOU WISH TO ABORT JOB AND
 * TRY AGAIN KEY ANY CHARACTER AND JOB WILL END
 * TO CONTINUE PROCESSING, HIT RETURN; OTHERWISE ENTER ANY CHARACTER:
 *
 * NUMBER OF LINES PRINTED PER PAGE [55]: 55
 *
 * @author Michael Stockman
 * @param purgeTerm
 * @param ptrm
 * @param rsts
 * @param tf
 * @return
 */
def parameterFile(def purgeTerm, def ptrm, def rsts, def t) {
    Path path = Paths.get(System.properties.'user.home','/gurjobs/PROD/waitlisting/sfpwait','sfpwait_' + purgeTerm + '_' + ptrm + '_' + t + '.parm')
    path.withWriter { bw ->
        bw.write '\n'
        bw << purgeTerm << '\n'
        bw << ptrm << '\n'
        bw << '\n'
        bw << rsts << '\n'
        bw << '\n'
        bw << 'U' << '\n'
        bw << 'Y' << '\n'
        bw << '\n'
        bw << '55'
    }
    return path
}

/**
 * List of terms and ptrms where the current date is after the WL status end date and exists waitlist registration and notifications that should be purged by the baseline process.
 * @param sql
 * @return
 */
def getWaitlistPurgeTerms(Sql sql) {
    def list = []
    sql.query(
            """select rrs.sfrrsts_term_code term
                         ,rrs.sfrrsts_ptrm_code ptrm
                         ,rrs.sfrrsts_rsts_code rsts
                     from sobwltc wl, sfrrsts rrs, stvterm tv
                    where rrs.sfrrsts_rsts_code = 'WL'
                      and rrs.sfrrsts_term_code = wl.sobwltc_term_code
                      and (exists (select 1 from sfrstcr r
                                    where r.sfrstcr_term_code = rrs.sfrrsts_term_code
                                      and r.sfrstcr_ptrm_code = rrs.sfrrsts_ptrm_code
                                      and r.sfrstcr_rsts_code = rrs.sfrrsts_rsts_code)
                       or exists (select 1 from sfrwlnt wlnt, ssbsect cs
                                   where wlnt.sfrwlnt_term_code = cs.ssbsect_term_code
                                     and wlnt.sfrwlnt_crn = cs.ssbsect_crn
                                     and cs.ssbsect_term_code = rrs.sfrrsts_term_code
                                     and cs.ssbsect_ptrm_code = rrs.sfrrsts_ptrm_code
                                     and cs.ssbsect_wait_capacity > 0)         
                          )  
                      and trunc(sysdate) > rrs.sfrrsts_end_date    
                      and tv.stvterm_code = rrs.sfrrsts_term_code
                      and trunc(sysdate) between stvterm_start_date and stvterm_end_date + 30"""
    ) { ResultSet rs ->
        while (rs.next()) {
            WaitlistPurgeRecord w =
                    new WaitlistPurgeRecord(
                            rs.getString('TERM'),
                            rs.getString('PTRM'),
                            rs.getString('RSTS')
                    )
            list.add(w)
        }
    }
    return list
}

/**
 * For historical purposes waitlist registration & notifications records should be saved to separate tables prior to Ellucian Banner purge process being run.
 * @param sql
 * @param term
 * @param ptrm
 */
void saveWaitlistDataBeforePurge(Sql sql, def term, def ptrm) {
    sql.call('{ call sykwlat.p_wl_student_save_data(?,?) }',[term,ptrm])
}

/**
 * Returns properties object with database credentials from file under user
 * @author Michael Stockman
 * @return Properties object
 */
Properties getDbProps() {
    Properties dbProps = new Properties()
    Paths.get(System.properties.'user.home','.credentials','bannerProduction.properties').withInputStream {
        dbProps.load(it)
    }
    return dbProps
}

/**
 * Pojo for waitlist purge term query information
 */
@Canonical
class WaitlistPurgeRecord {
    def purgeTerm
    def ptrm
    def rsts
}