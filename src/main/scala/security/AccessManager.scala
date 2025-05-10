package security

import metadata.DataProductMetadata
import org.apache.spark.sql.DataFrame

trait AccessPolicy {
  def canAccess(user: User, metadata: DataProductMetadata): Boolean
}

case class User(id: String, roles: List[String], teams: List[String])

class DomainBasedPolicy extends AccessPolicy {
  override def canAccess(user: User, metadata: DataProductMetadata): Boolean = {
    user.teams.contains(metadata.domain) || metadata.accessPolicies.exists(user.roles.contains)
  }
}

class AccessManager(policies: List[AccessPolicy]) {
  def checkAccess(user: User, metadata: DataProductMetadata): Boolean = {
    policies.exists(_.canAccess(user, metadata))
  }

  def filterData(user: User, df: DataFrame, metadata: DataProductMetadata): DataFrame = {
    if (checkAccess(user, metadata)) df else df.limit(0) // Возвращаем пустой DF если нет доступа
  }
}