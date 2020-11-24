<?php
/**
 * Default auth role
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM\Model\Auth;

use Modseven\ORM\ORM;

class Role extends ORM
{

    protected string $_table_name = 'roles';

    /**
     * Relationships
     * @var array
     */
    protected array $_has_many = [
        'users' => ['model' => User::class, 'through' => 'roles_users', 'foreign_key' => 'role_id'],
    ];

    /**
     * All rules
     * @return array
     */
    public function rules(): array
    {
        return [
            'name'        => [
                ['notEmpty'],
                ['minLength', [':value', 4]],
                ['maxLength', [':value', 32]],
            ],
            'description' => [
                ['maxLength', [':value', 255]],
            ]
        ];
    }
}